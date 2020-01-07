package it.usuratonkachi.kafka.reactor.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
public class ReactorStreamDispatcher<T> {

	private final ReactorKafkaConfiguration reactiveKafkaConfiguration;

	private Class<T> clazz;

	public ReactorStreamDispatcher(
			Class<T> clazz, ReactorKafkaProperties reactiveKafkaProperties, String labelTopicName){
		this.clazz = clazz;
		this.reactiveKafkaConfiguration = new ReactorKafkaConfiguration(reactiveKafkaProperties, labelTopicName);
	}

	public void listen(Function<Message<T>, Mono<Void>> function) {
		if (reactiveKafkaConfiguration.getConsumer() != null)
			reactiveKafkaConfiguration.getConsumer().receive()
					.groupBy(r -> r.receiverOffset().topicPartition())
					.flatMap(e -> e.buffer(1))
					.flatMap(receiverRecords -> {
						ReceiverRecord<byte[], byte[]> r = receiverRecords.get(0);
						try {
							Message<T> message = receiverRecordToMessage(r);
							return function.apply(message)
									.doOnNext(e -> {
										r.receiverOffset().acknowledge();
										r.receiverOffset().commit();
									});
							// TODO In case of businessException should acknowledge and commit.
						} catch (Exception ex) {
							log.debug("Exception found, message not committed nor acknowledged, will be retried in minutes: " + ex.getMessage(), ex);
						}
						return Mono.empty();
					})
					.subscribe();
		else
			throw new RuntimeException("No consumer options for topic with label "+ reactiveKafkaConfiguration.getTopicName() + " configured!");
	}

	public Disposable send(Message<T> message){
		return sendAsync(message).subscribe();
	}

	private Flux<SenderResult<Object>> sendAsync(Message<T> message) {
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
			Iterable<Header> headers = headersToProducerRecordHeaders(message);
			return new ProducerRecord<>(reactiveKafkaConfiguration.getTopicName(), null, null, null, payload, headers);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	private Iterable<Header> headersToProducerRecordHeaders(Message<T> message){
		ObjectMapper objectMapper = new ObjectMapper();
		return message.getHeaders().entrySet().stream()
				.filter(entryHeader -> entryHeader.getValue() instanceof Serializable)
				.map(entryHeader -> {
					if (entryHeader.getValue() instanceof byte[])
						return new RecordHeader(entryHeader.getKey(), (byte[])entryHeader.getValue());
					else {
						try {
							return new RecordHeader(entryHeader.getKey(), objectMapper.writeValueAsBytes(entryHeader.getValue()));
						} catch (JsonProcessingException e) {
							throw new RuntimeException(e);
						}
					}
				})
				.collect(Collectors.toList());
	}

	private Message<T> receiverRecordToMessage(ReceiverRecord<byte[], byte[]> receiverRecord) {
		Map<String, Object> headersMap = receiverRecordToHeaders(receiverRecord);
		MessageHeaders headers = new MessageHeaders(headersMap);
		return receiverRecordToMessage(receiverRecord, headers);
	}

	@SuppressWarnings("unchecked")
	private Message<T> receiverRecordToMessage(ReceiverRecord<byte[], byte[]> receiverRecord, MessageHeaders messageHeaders) {
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

	private Map<String, Object> receiverRecordToHeaders(ReceiverRecord<byte[], byte[]> receiverRecord){
		return StreamSupport.stream(receiverRecord.headers().spliterator(), false)
				.map(header -> {
					//Object headerValue = new String(header.value());
					//return Tuples.of(header.key(), headerValue);
					return Tuples.of(header.key(), header.value());
				})
				.collect(Collectors.groupingBy(Tuple2::getT1, Collectors.mapping(Tuple2::getT2, toSingleton())));
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
