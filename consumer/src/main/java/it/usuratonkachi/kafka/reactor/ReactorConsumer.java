package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.data.service.KafkaService;
import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.annotation.ReactorStreamListener;
import it.usuratonkachi.kafka.reactor.streamconfig.Streams;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

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

	@ReactorStreamListener(Streams.MAIL_CHANNEL_INPUT)
	public Mono<Void> mailListener(@Payload Mail mail, @Headers Map<String, Object> headers){
		return Mono.just(mail)
				.doOnNext(m -> kafkaService.ackIfNotYetLogOtherwise(m.getMsgNum(), m.getProducer(), m.getClass().getSimpleName()))
				.flatMap(e -> Mono.empty());
	}

	@ReactorStreamListener(Streams.MESSAGE_CHANNEL_INPUT)
	public Mono<Void> mailListener(@Payload Message message, @Headers Map<String, Object> headers){
		return Mono.just(message)
				.doOnNext(m -> kafkaService.ackIfNotYetLogOtherwise(m.getMsgNum(), m.getProducer(), m.getClass().getSimpleName()))
				.flatMap(e -> Mono.empty());
	}

	@ReactorStreamListener(Streams.SMS_CHANNEL_INPUT)
	public Mono<Void> mailListener(@Payload Sms sms, @Headers Map<String, Object> headers){
		return Mono.just(sms)
				.doOnNext(m -> kafkaService.ackIfNotYetLogOtherwise(m.getMsgNum(), m.getProducer(), m.getClass().getSimpleName()))
				.flatMap(e -> Mono.empty());
	}

	@ReactorStreamListener(Streams.MMS_CHANNEL_INPUT)
	public Mono<Void> mailListener(@Payload Mms mms, @Headers Map<String, Object> headers){
		return Mono.just(mms)
				.doOnNext(m -> kafkaService.ackIfNotYetLogOtherwise(m.getMsgNum(), m.getProducer(), m.getClass().getSimpleName()))
				.flatMap(e -> Mono.empty());
	}

}
