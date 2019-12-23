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
@EnableBinding(Processor.class)
@RequiredArgsConstructor
public class SCSConsumer {

	private final KafkaService kafkaService;

	@StreamListener(Streams.MAIL_CHANNEL_INPUT)
	public void onMail(@Payload Mail msg, @Headers Map<String, Object> headers) {
		long waittime = 10000L;
		final Mono<Mail> mono = Mono.just(msg)
				.doOnNext(e -> kafkaService.ackIfNotYetLogOtherwise(msg.getMsgNum(), msg.getProducer(), msg.getClass().getSimpleName()))
				.delayElement(Duration.of(waittime, ChronoUnit.SECONDS))
				.flatMap(x -> Mono.defer(() -> Mono.just(msg)));
		mono.block();
	}

	@StreamListener(Streams.MESSAGE_CHANNEL_INPUT)
	public void onMessage(@Payload Message msg, @Headers Map<String, Object> headers) {
		long waittime = 0L;
		final Mono<Message> mono = Mono.just(msg)
				.doOnNext(e -> kafkaService.ackIfNotYetLogOtherwise(msg.getMsgNum(), msg.getProducer(), msg.getClass().getSimpleName()))
				.delayElement(Duration.of(waittime, ChronoUnit.SECONDS))
				.flatMap(x -> Mono.defer(() -> Mono.just(msg)));
		mono.block();
	}

	@StreamListener(Streams.MMS_CHANNEL_INPUT)
	public void onMms(@Payload Mms msg, @Headers Map<String, Object> headers) {
		long waittime = 0L;
		final Mono<Mms> mono = Mono.just(msg)
				.doOnNext(e -> kafkaService.ackIfNotYetLogOtherwise(msg.getMsgNum(), msg.getProducer(), msg.getClass().getSimpleName()))
				.delayElement(Duration.of(waittime, ChronoUnit.SECONDS))
				.flatMap(x -> Mono.defer(() -> Mono.just(msg)));
		mono.block();
	}

	@StreamListener(Streams.SMS_CHANNEL_INPUT)
	public void onSms(@Payload Sms msg, @Headers Map<String, Object> headers) {
		long waittime = 0L;
		final Mono<Sms> mono = Mono.just(msg)
				.doOnNext(e -> kafkaService.ackIfNotYetLogOtherwise(msg.getMsgNum(), msg.getProducer(), msg.getClass().getSimpleName()))
				.delayElement(Duration.of(waittime, ChronoUnit.SECONDS))
				.flatMap(x -> Mono.defer(() -> Mono.just(msg)));
		mono.block();
	}

}
