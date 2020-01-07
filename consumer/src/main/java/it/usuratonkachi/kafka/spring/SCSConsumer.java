package it.usuratonkachi.kafka.spring;

import it.usuratonkachi.kafka.data.service.KafkaService;
import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.spring.streamconfig.Streams;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Processor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class SCSConsumer {

	private final KafkaService kafkaService;

	//@Value("${default.waittime:1000}")
	private Long waittime = 1000L;

	@StreamListener(Streams.MAIL_CHANNEL_INPUT)
	public void onMail(@Payload Mail msg, @Headers Map<String, Object> headers) {
		final Mono<Mail> mono = Mono.just(msg)
				.doOnNext(e -> System.out.println("Start: " + msg.getMsgNum()))
				//.doOnNext(e -> kafkaService.ackIfNotYetLogOtherwise(msg.getMsgNum(), msg.getProducer(), msg.getClass().getSimpleName()))
				.delayElement(Duration.of(waittime, ChronoUnit.MILLIS))
				.doOnNext(e -> System.out.println("Ending: " + msg.getMsgNum()))
				.flatMap(x -> Mono.defer(() -> Mono.just(msg)));
		mono.subscribe();
	}

	@StreamListener(Streams.MESSAGE_CHANNEL_INPUT)
	public void onMessage(@Payload Message msg, @Headers Map<String, Object> headers) {
		final Mono<Message> mono = Mono.just(msg)
				.doOnNext(e -> System.out.println("Start: " + msg.getMsgNum()))
				//.doOnNext(e -> kafkaService.ackIfNotYetLogOtherwise(msg.getMsgNum(), msg.getProducer(), msg.getClass().getSimpleName()))
				.delayElement(Duration.of(waittime, ChronoUnit.MILLIS))
				.doOnNext(e -> System.out.println("Ending: " + msg.getMsgNum()))
				.flatMap(x -> Mono.defer(() -> Mono.just(msg)));
		mono.subscribe();
	}

	@StreamListener(Streams.MMS_CHANNEL_INPUT)
	public void onMms(@Payload Mms msg, @Headers Map<String, Object> headers) {
		final Mono<Mms> mono = Mono.just(msg)
				.doOnNext(e -> System.out.println("Start: " + msg.getMsgNum()))
				//.doOnNext(e -> kafkaService.ackIfNotYetLogOtherwise(msg.getMsgNum(), msg.getProducer(), msg.getClass().getSimpleName()))
				.delayElement(Duration.of(waittime, ChronoUnit.MILLIS))
				.doOnNext(e -> System.out.println("Ending: " + msg.getMsgNum()))
				.flatMap(x -> Mono.defer(() -> Mono.just(msg)));
		mono.subscribe();
	}

	@StreamListener(Streams.SMS_CHANNEL_INPUT)
	public void onSms(@Payload Sms msg, @Headers Map<String, Object> headers) {
		final Mono<Sms> mono = Mono.just(msg)
				.doOnNext(e -> System.out.println("Start: " + msg.getMsgNum()))
				//.doOnNext(e -> kafkaService.ackIfNotYetLogOtherwise(msg.getMsgNum(), msg.getProducer(), msg.getClass().getSimpleName()))
				.delayElement(Duration.of(waittime, ChronoUnit.MILLIS))
				.doOnNext(e -> System.out.println("Ending: " + msg.getMsgNum()))
				.flatMap(x -> Mono.defer(() -> Mono.just(msg)));
		mono.subscribe();
	}

}
