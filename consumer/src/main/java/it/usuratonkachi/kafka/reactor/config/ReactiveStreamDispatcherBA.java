package it.usuratonkachi.kafka.reactor.config;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReactiveStreamDispatcherBA<T> {

	private final ReactiveKafkaConfigurationBA reactiveKafkaConfiguration;

	private Class<T> clazz;

	@SuppressWarnings("unchecked")
	private Class<T> getGenericTypeAtRuntime(){
		return (Class<T>) getClass();
	}

	public ReactiveStreamDispatcherBA(ReactiveKafkaProperties reactiveKafkaProperties, String labelTopicName){
		this.clazz = getGenericTypeAtRuntime();
		this.reactiveKafkaConfiguration = new ReactiveKafkaConfigurationBA(reactiveKafkaProperties, labelTopicName);
	}

	public ReactiveStreamDispatcherBA(ReactiveKafkaConfigurationBA reactiveKafkaConfiguration) {
		this.clazz = getGenericTypeAtRuntime();
		this.reactiveKafkaConfiguration = reactiveKafkaConfiguration;
	}

	public Flux<Message<T>> _listen() {

		if (reactiveKafkaConfiguration.getConsumer() != null)
			return reactiveKafkaConfiguration.getConsumer().receive().map(this::receiverRecordToMessage);
		else
			throw new RuntimeException("No consumer options for topic with label "+ reactiveKafkaConfiguration.getTopicName() + " configured!");
	}

	public Flux<Message<T>> listen() {
		if (reactiveKafkaConfiguration.getConsumer() != null)
			return reactiveKafkaConfiguration.getConsumer().receiveAutoAck().flatMap(e -> e).map(this::consumerRecordToMessage);
		else
			throw new RuntimeException("No consumer options for topic with label "+ reactiveKafkaConfiguration.getTopicName() + " configured!");
	}

	public KafkaReceiver<String, byte[]> listener() {
		if (reactiveKafkaConfiguration.getConsumer() != null)
			return reactiveKafkaConfiguration.getConsumer();
		else
			throw new RuntimeException("No consumer options for topic with label "+ reactiveKafkaConfiguration.getTopicName() + " configured!");
	}

	public Flux<SenderResult<Object>> send(Message<byte[]> message) {
		if (reactiveKafkaConfiguration.getProducer() != null) {
			return internalSend(message);
		} else {
			throw new RuntimeException(
					"No producer options for topic with label " + reactiveKafkaConfiguration.getTopicName() + " configured!");
		}
	}

	private Flux<SenderResult<Object>> internalSend(Message<byte[]> message){
		ProducerRecord<String, byte[]> producer = messageToProducerRecord(message);
		SenderRecord<String, byte[], Object> senderRecord = SenderRecord.create(producer, null);
		Flux<SenderRecord<String, byte[], Object>> messageSource = Flux.from(Mono.defer(() -> Mono.just(senderRecord)));
		return reactiveKafkaConfiguration.getProducer().send(messageSource);
	}

	private ProducerRecord<String, byte[]> messageToProducerRecord(Message<byte[]> message){
		return new ProducerRecord<>(reactiveKafkaConfiguration.getTopicName(), null, null, null, message.getPayload(), headersToProducerRecordHeaders(message));
	}

	private Iterable<Header> headersToProducerRecordHeaders(Message<byte[]> message){
		return message.getHeaders().entrySet().stream()
				.filter(entryHeader -> entryHeader.getValue() instanceof Serializable)
				.map(entryHeader -> new RecordHeader(entryHeader.getKey(), SerializationUtils.serialize((Serializable) entryHeader.getValue())))
				.collect(Collectors.toList());
	}

	private Message<T> receiverRecordToMessage(ReceiverRecord<String, byte[]> receiverRecord) {
		MessageHeaders messageHeaders = receiverRecordToHeaders(receiverRecord);
		T deserializedValue = SerializationUtils.deserialize(receiverRecord.value());
		return new GenericMessage<T>(deserializedValue, messageHeaders);
	}

	private Message<T> consumerRecordToMessage(ConsumerRecord<String, byte[]> consumerRecord){
		MessageHeaders messageHeaders = receiverRecordToHeaders(consumerRecord);
		T deserializedValue = SerializationUtils.deserialize(consumerRecord.value());
		return new GenericMessage<T>(deserializedValue, messageHeaders);
	}

	private MessageHeaders receiverRecordToHeaders(ReceiverRecord<String, byte[]> receiverRecord){
		Map<String, Object> headersMap = StreamSupport.stream(receiverRecord.headers().spliterator(), false)
				.map(header -> {
					Object headerValue = SerializationUtils.deserialize(header.value());
					return Tuples.of(header.key(), headerValue);
				})
				.collect(Collectors.groupingBy(Tuple2::getT1, Collectors.mapping(Tuple2::getT2, toSingleton())));
		return new MessageHeaders(headersMap);
	}

	private MessageHeaders receiverRecordToHeaders(ConsumerRecord<String, byte[]> consumerRecord){
		Map<String, Object> headersMap = StreamSupport.stream(consumerRecord.headers().spliterator(), false)
				.map(header -> {
					try {
						Object headerValue = SerializationUtils.deserialize(header.value());
						return Tuples.of(header.key(), headerValue);
					} catch (Exception ex){
						throw new RuntimeException(ex);
					}
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
