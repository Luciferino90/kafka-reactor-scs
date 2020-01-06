package it.usuratonkachi.kafka.reactor.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class ReactiveStreamDispatcher<T> {

	private final ReactiveKafkaConfiguration reactiveKafkaConfiguration;

	private Class<T> clazz;

	public ReactiveStreamDispatcher(Class<T> clazz, ReactiveKafkaProperties reactiveKafkaProperties, String labelTopicName){
		this.clazz = clazz;
		this.reactiveKafkaConfiguration = new ReactiveKafkaConfiguration(reactiveKafkaProperties, labelTopicName);
	}

	public Flux<Message<T>> _listen() {
		if (reactiveKafkaConfiguration.getConsumer() != null)
			return reactiveKafkaConfiguration.getConsumer().receive().map(this::receiverRecordToMessage);
		else
			throw new RuntimeException("No consumer options for topic with label "+ reactiveKafkaConfiguration.getTopicName() + " configured!");
	}

	public Flux<Message<T>> listen() {
		if (reactiveKafkaConfiguration.getConsumer() != null)
			return reactiveKafkaConfiguration.getConsumer().receive().map(this::receiverRecordToMessage);
		else
			throw new RuntimeException("No consumer options for topic with label "+ reactiveKafkaConfiguration.getTopicName() + " configured!");
	}

	public void listen(Function<T, Void> function) {
		if (reactiveKafkaConfiguration.getConsumer() != null)
			reactiveKafkaConfiguration.getConsumer().receive()
					.doOnNext(r -> {
						try {
							Message<T> message = receiverRecordToMessage(r);
							function.apply(message.getPayload());
							r.receiverOffset().acknowledge();
							r.receiverOffset().commit();
							// TODO In case of businessException should acknowledge and commit.
						} catch (Exception ex) {
							log.debug("Exception found, message not committed nor acknowledged, will be retried in minutes: " + ex.getMessage(), ex);
						}
					})
					.subscribe();
		else
			throw new RuntimeException("No consumer options for topic with label "+ reactiveKafkaConfiguration.getTopicName() + " configured!");
	}

	public void _listen(Function<Message<T>, Void> function) {
		if (reactiveKafkaConfiguration.getConsumer() != null)
			reactiveKafkaConfiguration.getConsumer().receive()
					.doOnNext(r -> {
						Message<T> message = receiverRecordToMessage(r);
						function.apply(message);
						r.receiverOffset().acknowledge();
					})
					.subscribe();
		else
			throw new RuntimeException("No consumer options for topic with label "+ reactiveKafkaConfiguration.getTopicName() + " configured!");
	}

	public KafkaReceiver<byte[], byte[]> listener() {
		if (reactiveKafkaConfiguration.getConsumer() != null)
			return reactiveKafkaConfiguration.getConsumer();
		else
			throw new RuntimeException("No consumer options for topic with label "+ reactiveKafkaConfiguration.getTopicName() + " configured!");
	}

	public Flux<SenderResult<Object>> send(Message<T> message) {
		if (reactiveKafkaConfiguration.getProducer() != null) {
			return internalSend(message);
		} else {
			throw new RuntimeException(
					"No producer options for topic with label " + reactiveKafkaConfiguration.getTopicName() + " configured!");
		}
	}

	private Flux<SenderResult<Object>> internalSend(Message<T> message){
		ProducerRecord<byte[], byte[]> producer = messageToProducerRecord(message);
		SenderRecord<byte[], byte[], Object> senderRecord = SenderRecord.create(producer, null);
		Flux<SenderRecord<byte[], byte[], Object>> messageSource = Flux.from(Mono.defer(() -> Mono.just(senderRecord)));
		return reactiveKafkaConfiguration.getProducer().send(messageSource);
	}

	private ProducerRecord<byte[], byte[]> messageToProducerRecord(Message<T> message){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			byte[] payload = objectMapper.writeValueAsBytes(message.getPayload());
			return new ProducerRecord<>(reactiveKafkaConfiguration.getTopicName(), null, null, null, payload, headersToProducerRecordHeaders(message));
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	private Iterable<Header> headersToProducerRecordHeaders(Message<T> message){
		return message.getHeaders().entrySet().stream()
				.filter(entryHeader -> entryHeader.getValue() instanceof Serializable)
				.map(entryHeader -> new RecordHeader(entryHeader.getKey(), SerializationUtils.serialize((Serializable) entryHeader.getValue())))
				.collect(Collectors.toList());
	}

	@SuppressWarnings("unchecked")
	private Message<T> receiverRecordToMessage(ReceiverRecord<byte[], byte[]> receiverRecord) {
		MessageHeaders messageHeaders = receiverRecordToHeaders(receiverRecord);
		T deserializedValue = null;
		try {
			Class<T> c = (Class<T>) Class.forName((String) Optional.ofNullable(messageHeaders.get("__TypeId__")).orElse(clazz.getName()));
			deserializedValue = deserializeObject(receiverRecord.value(), c);
		} catch (ClassNotFoundException e) {
			log.error("Could not deserialize class " + messageHeaders.get("__TypeId__"));
			deserializeObject(receiverRecord.value());
		}
		assert deserializedValue != null;
		return new GenericMessage<>(deserializedValue, messageHeaders);
	}


	private T deserializeObject(byte[] serialized){
		return deserializeObject(serialized, clazz);
	}

	@SuppressWarnings("unchecked")
	private T deserializeObject(byte[] serialized, Class<T> clazz){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			Map<String, Object> content = objectMapper.readValue(serialized, Map.class);
			if (content != null && content.containsKey("payload"))
				return objectMapper.convertValue(content.get("payload"), clazz);
			else
				return objectMapper.convertValue(content, clazz);
		} catch (Exception ex) {
			String errorMessage = "Cannot deserialize " + new String(serialized);
			log.error(errorMessage, ex);
			throw new RuntimeException(errorMessage, ex);
		}
	}

	private MessageHeaders receiverRecordToHeaders(ReceiverRecord<byte[], byte[]> receiverRecord){
		Map<String, Object> headersMap = StreamSupport.stream(receiverRecord.headers().spliterator(), false)
				.map(header -> {
					Object headerValue = new String(header.value());
					return Tuples.of(header.key(), headerValue);
				})
				.collect(Collectors.groupingBy(Tuple2::getT1, Collectors.mapping(Tuple2::getT2, toSingleton())));
		return new MessageHeaders(headersMap);
	}

	public static <T> Collector<T, ?, T> toSingleton() {
		return Collectors.collectingAndThen(
				Collectors.toList(),
				list -> {
					if (list.size() != 1) {
						throw new RuntimeException("More than one result");
					}
					return list.get(0);
				}
		);
	}

}
