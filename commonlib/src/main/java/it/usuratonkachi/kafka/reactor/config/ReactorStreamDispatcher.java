package it.usuratonkachi.kafka.reactor.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.core.GenericTypeResolver;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Classe che di occupa di leggere e inviare messaggi verso kafka.
 * Potenzialmente un solo bean può essere configurato sia come consumer che come producer.
 *
 * @param <T> Tipo di messaggio che si invia e si riceve attraverso questo topic
 */
@Slf4j
public class ReactorStreamDispatcher<T> implements MessageChannel {

	private final ReactorKafkaConfiguration reactorKafkaConfiguration;

	private Class<T> clazz;
	private ObjectMapper objectMapper;
	private boolean alreadyStarted = false;

	/**
	 * Costruttore del dispatcher
	 * @param clazz
	 * 		classe del tipo di messaggio in invio e ricezione
	 * @param reactiveKafkaProperties
	 * 		properties autoconfiguranti provenienti da Spring Cloud Stream
	 * @param labelTopicName
	 * 		label del topic da configurare. Viene matchato con il `channel` di spring.cloud.stream.bindings.channel
	 * 		e spring.cloud.stream.kafka.bindings.channel per recuperare le properties da configurare
	 */
	public ReactorStreamDispatcher(Class<T> clazz, ReactorKafkaProperties reactiveKafkaProperties, String labelTopicName){
		this.clazz = clazz;
		this.objectMapper = reactiveKafkaProperties.getObjectMapper();
		this.reactorKafkaConfiguration = new ReactorKafkaConfiguration(reactiveKafkaProperties, labelTopicName);
	}

	public void listen(Function<Message<T>, Mono<Void>> function) {
		if (alreadyStarted) return;
		else alreadyStarted = true;
		ReactorConsumer consumer = reactorKafkaConfiguration.getConsumer();
		if (consumer != null) {
			if (consumer.hasManualAck())
				listenAtleastOnce(function, consumer);
			else
				listenAtmostOnce(function, consumer);
		} else {
			throw new RuntimeException(
					"No consumer options for topic with label " + reactorKafkaConfiguration.getTopicName() + " configured!");
		}
	}

	/**
	 * Metodo di ascolto del listener. Si registra una sola volta a causa della doppia lettura delle classi da parte di spring
	 * (Component e Component$Proxy).
	 *
	 * Supporta la parallelizzazione dei messaggi tramite la property
	 * spring.cloud.stream.kafka.bindings.channel.consumer.configuration.concurrency
	 *
	 * L'ordinamento per partizione migliora la stabilità della lettura dei messaggi, riducendo eventuali gestioni multiple.
	 *
	 *
	 * Metodo che si occupa di gestire il singolo messaggio eseguendo la function passata da chi utilizza la libreria.
	 *
	 *  Per poter gestire l'ack e il commit dei messaggi ci si aspetta che il consumer torni alla libreria una qualche forma
	 * di informazione, per il momento si è optato per un Mono.empty.
	 *
	 * Di default tutte le eccezioni diverse da BusinessException riportano l'errore a Kafka che ritrasmette il messaggio.
	 *
	 * Le business exception invece committano l'ack.
	 *
	 * @param function
	 * 		function da eseguire su ciascun messaggio
	 */
	private void listenAtleastOnce(Function<Message<T>, Mono<Void>> function, ReactorConsumer consumer) {
		Map<Integer, Long> partitionOffsetDuplicates = new ConcurrentHashMap<>();
		Integer concurrency = reactorKafkaConfiguration.getConcurrency();
		consumer.receive()
				.buffer(concurrency)
				.concatMap(receiverRecords -> Flux.fromIterable(receiverRecords)
						.filter(r -> {
							Optional<Long> oldOffset = Optional.ofNullable(partitionOffsetDuplicates.get(r.partition()));
							return oldOffset.isEmpty() || oldOffset.get() < r.offset();
						})
						.switchIfEmpty(Mono.defer(() -> {
							log.debug("Message alrerady managed by this consumer. Skipped.");
							return Mono.empty();
						}))
						.filter(r -> consumer.hasPartitionAssigned(r.partition()))
						.switchIfEmpty(Mono.defer(() -> {
							log.debug("Partition revoked, cannot commit message. Skipped.");
							return Mono.empty();
						}))
						.doOnNext(receiverRecord -> partitionOffsetDuplicates.put(receiverRecord.partition(), receiverRecord.offset()))
						.doOnNext(consumer::toBeAcked)
						.flatMap(receiverRecord -> {
							try {
								return function.apply(receiverRecordToMessage(receiverRecord))
										.switchIfEmpty(Mono.defer(() -> {
											if (consumer.hasPartitionAssigned(receiverRecord.partition())) {
												consumer.ackRecord(receiverRecord);
											}
											return Mono.empty();
										}))
										.doOnError(RuntimeException.class, businessException -> {
											if (consumer.hasPartitionAssigned(receiverRecord.partition())) {
												consumer.ackRecord(receiverRecord);
											}
										});
							} catch (Exception ex) {
								log.debug("Exception found, message not committed nor acknowledged, will be retried in minutes: " + ex.getMessage(), ex);
							}
							return Mono.empty();
						})
				)
				.doOnError(Throwable::printStackTrace)
				.subscribe();

	}

	private void listenAtmostOnce(Function<Message<T>, Mono<Void>> function, ReactorConsumer consumer) {
		Map<Integer, Long> partitionOffsetDuplicates = new ConcurrentHashMap<>();
		Integer concurrency = reactorKafkaConfiguration.getConcurrency();
		consumer.receiveAtmostOnce()
				.buffer(concurrency)
				.concatMap(consumerRecords -> Flux.fromIterable(consumerRecords)
						.filter(r -> {
							Optional<Long> oldOffset = Optional.ofNullable(partitionOffsetDuplicates.get(r.partition()));
							return oldOffset.isEmpty() || oldOffset.get() < r.offset();
						})
						.switchIfEmpty(Mono.defer(() -> {
							log.debug("Message alrerady managed by this consumer. Skipped.");
							return Mono.empty();
						}))
						.filter(r -> consumer.hasPartitionAssigned(r.partition()))
						.switchIfEmpty(Mono.defer(() -> {
							log.debug("Partition revoked, cannot commit message. Skipped.");
							return Mono.empty();
						}))
						.doOnNext(receiverRecord -> partitionOffsetDuplicates.put(receiverRecord.partition(), receiverRecord.offset()))
						.flatMap(consumerRecord -> {
							try {
								return function.apply(consumerRecordToMessage(consumerRecord));
							} catch (Exception ex) {
								log.debug("Exception found, message not committed nor acknowledged, will be retried in minutes: " + ex.getMessage(), ex);
							}
							return Mono.empty();
						})
				)
				.doOnError(Throwable::printStackTrace)
				.subscribe();
	}

	/**
	 * Wrapper invio messaggi da richiamare nei progetti
	 * @param message
	 * @return
	 */
	public boolean send(Message<?> message, long timeout){
		sendAsync(message).subscribe();
		return true;
	}

	/**
	 * Invio asincrono dei messaggi
	 * @param message
	 * @return
	 */
	private Flux<SenderResult<Object>> sendAsync(Message<?> message) {
		if (reactorKafkaConfiguration.getProducer() != null) {
			return internalSend(message);
		} else {
			throw new RuntimeException(
					"No producer options for topic with label " + reactorKafkaConfiguration.getTopicName() + " configured!");
		}
	}

	/**
	 * Converte il messaggio in byte e lo invia verso il topic kafka.
	 * La libreria supporta invii molteplici tramite flux, cosa che nel nostro caso non è richiesta, gestiamo solamente
	 * l'invio dei singoli messaggi.
	 *
	 * @param message
	 * @return
	 */
	private Flux<SenderResult<Object>> internalSend(Message<?> message){
		ProducerRecord<byte[], byte[]> producer = messageToProducerRecord(message);
		SenderRecord<byte[], byte[], Object> senderRecord = SenderRecord.create(producer, null);
		Flux<SenderRecord<byte[], byte[], Object>> messageSource = Flux.from(Mono.defer(() -> Mono.just(senderRecord)));
		return reactorKafkaConfiguration.getProducer().send(messageSource);
	}

	/**
	 * Metodo per convertire i message in ProducerRecord.
	 * @param message
	 * @return
	 */
	private ProducerRecord<byte[], byte[]> messageToProducerRecord(Message<?> message){
		try {
			byte[] payload = objectMapper.writeValueAsBytes(message.getPayload());
			Iterable<Header> headers = headersToProducerRecordHeaders(message);
			return new ProducerRecord<>(reactorKafkaConfiguration.getTopicName(), null, null, null, payload, headers);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Metodo che converte gli header di un message in RecordHeader
	 * @param message
	 * @return
	 */
	private Iterable<Header> headersToProducerRecordHeaders(Message<?> message){
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

	/**
	 * Metodo che deserializza un messaggio in byte array in un Message del tipo configurato nel dispatcher.
	 * @param receiverRecord
	 * @return
	 */
	private Message<T> receiverRecordToMessage(ReceiverRecord<byte[], byte[]> receiverRecord) {
		Map<String, Object> headersMap = receiverRecordToHeaders(receiverRecord);
		MessageHeaders headers = new MessageHeaders(headersMap);
		return receiverRecordToMessage(receiverRecord, headers);
	}

	/**
	 * Metodo che deserializza un messaggio in byte array in un Message del tipo configurato nel dispatcher.
	 * @param receiverRecord
	 * @return
	 */
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

	/**
	 * Metodo che deserializza gli header in byte array in una mappa.
	 * @param receiverRecord
	 * @return
	 */
	private Map<String, Object> consumerRecordToHeaders(ConsumerRecord<byte[], byte[]> receiverRecord){
		return StreamSupport.stream(receiverRecord.headers().spliterator(), false)
				.map(header -> Tuples.of(header.key(), header.value()))
				.collect(Collectors.groupingBy(Tuple2::getT1, Collectors.mapping(Tuple2::getT2, toSingleton())));
	}

	/**
	 * Metodo che deserializza un messaggio in byte array in un Message del tipo configurato nel dispatcher.
	 * @param consumerRecord
	 * @return
	 */
	private Message<T> consumerRecordToMessage(ConsumerRecord<byte[], byte[]> consumerRecord) {
		Map<String, Object> headersMap = consumerRecordToHeaders(consumerRecord);
		MessageHeaders headers = new MessageHeaders(headersMap);
		return consumerRecordToMessage(consumerRecord, headers);
	}

	/**
	 * Metodo che deserializza un messaggio in byte array in un Message del tipo configurato nel dispatcher.
	 * @param consumerRecord
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Message<T> consumerRecordToMessage(ConsumerRecord<byte[], byte[]> consumerRecord, MessageHeaders messageHeaders) {
		T deserializedValue = null;
		try {
			Class<T> c = (Class<T>) Class.forName((String) Optional.ofNullable(messageHeaders.get("__TypeId__")).orElse(clazz.getName()));
			deserializedValue = deserializeObject(consumerRecord.value(), c);
		} catch (ClassNotFoundException e) {
			log.error("Could not deserialize class " + messageHeaders.get("__TypeId__"));
			deserializeObject(consumerRecord.value());
		}
		assert deserializedValue != null;
		return new GenericMessage<>(deserializedValue, messageHeaders);
	}

	/**
	 * Metodo che deserializza gli header in byte array in una mappa.
	 * @param receiverRecord
	 * @return
	 */
	private Map<String, Object> receiverRecordToHeaders(ReceiverRecord<byte[], byte[]> receiverRecord){
		return StreamSupport.stream(receiverRecord.headers().spliterator(), false)
				.map(header -> Tuples.of(header.key(), header.value()))
				.collect(Collectors.groupingBy(Tuple2::getT1, Collectors.mapping(Tuple2::getT2, toSingleton())));
	}

	@SuppressWarnings("unchecked")
	private T deserializeObject(byte[] serialized){
		return deserializeObject(serialized, clazz);
	}

	@SuppressWarnings("unchecked")
	private T deserializeObject(byte[] serialized, Class<T> clazz){
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
