package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.data.service.KafkaService;
import it.usuratonkachi.kafka.dto.*;
import it.usuratonkachi.kafka.reactor.config.annotation.input.ReactorStreamListener;
import it.usuratonkachi.kafka.reactor.streamconfig.Streams;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class ReactorConsumer {

	@Value("${spring.profiles:default}")
	private String profile;

	//@Value("${default.waittime:10000}")
	private Long waittime = 0L;

	@Autowired
	private KafkaService kafkaService;

	@ReactorStreamListener(Streams.NOTIFICATION_CHANNEL_INPUT)
	public Mono<Void> notificationListener(
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(value = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
			@Headers Map<String, Object> headers,
			@Payload Notification notification
	){
		return Mono.just(notification)
				.flatMap(not -> Mono.empty());
	}

	@ReactorStreamListener(Streams.MAIL_CHANNEL_INPUT)
	public Mono<Void> mailListener(@Payload Mail mail, @Headers Map<String, Object> headers){
		return Mono.just(mail)
				//.delayElement(Duration.ofHours(10))
				.doOnNext(m -> kafkaService.ackIfNotYetLogOtherwise(m.getMsgNum(), m.getProducer(), m.getClass().getSimpleName()))
				.flatMap(e -> Mono.empty());
	}

	@ReactorStreamListener(Streams.MESSAGE_CHANNEL_INPUT)
	public Mono<Void> mailListener(@Payload Message message, @Headers Map<String, Object> headers){
		return Mono.just(message)
				//.delayElement(Duration.ofHours(10))
				.doOnNext(m -> kafkaService.ackIfNotYetLogOtherwise(m.getMsgNum(), m.getProducer(), m.getClass().getSimpleName()))
				.flatMap(e -> Mono.empty());
	}

	@ReactorStreamListener(Streams.SMS_CHANNEL_INPUT)
	public Mono<Void> mailListener(@Payload Sms sms, @Headers Map<String, Object> headers){
		return Mono.just(sms)
				//.delayElement(Duration.ofHours(10))
				.doOnNext(m -> kafkaService.ackIfNotYetLogOtherwise(m.getMsgNum(), m.getProducer(), m.getClass().getSimpleName()))
				.flatMap(e -> Mono.empty());
	}

	@ReactorStreamListener(Streams.MMS_CHANNEL_INPUT)
	public Mono<Void> mailListener(@Payload Mms mms, @Headers Map<String, Object> headers){
		return Mono.just(mms)
				//.delayElement(Duration.ofHours(10))
				.doOnNext(m -> kafkaService.ackIfNotYetLogOtherwise(m.getMsgNum(), m.getProducer(), m.getClass().getSimpleName()))
				.flatMap(e -> Mono.empty());
	}

}
