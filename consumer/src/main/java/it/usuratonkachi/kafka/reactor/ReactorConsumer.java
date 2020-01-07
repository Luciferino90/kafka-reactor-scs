package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.data.service.KafkaService;
import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactorStreamDispatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.function.Function;

@Component
@Slf4j
@RequiredArgsConstructor
public class ReactorConsumer {

	@Value("${spring.profiles:default}")
	private String profile;

	//@Value("${default.waittime:10000}")
	private Long waittime = 0L;

	private static Boolean started = false;

	private final ReactorStreamDispatcher<Mail> mailDispatcher;
	private final ReactorStreamDispatcher<Message> messageDispatcher;
	private final ReactorStreamDispatcher<Mms> mmsDispatcher;
	private final ReactorStreamDispatcher<Sms> smsDispatcher;

	@Autowired
	private KafkaService kafkaService;

	Function<org.springframework.messaging.Message<Mail>, Mono<Void>> mailListener = kafkaMessage -> {
		Mail payload = kafkaMessage.getPayload();
		MessageHeaders headers = kafkaMessage.getHeaders();
		kafkaService.ackIfNotYetLogOtherwise(payload.getMsgNum(), payload.getProducer(), payload.getClass().getSimpleName());
		try {
			Thread.sleep(waittime);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
		return null;
	};

	Function<org.springframework.messaging.Message<Message>, Mono<Void>> messageListener = kafkaMessage -> {
		Message payload = kafkaMessage.getPayload();
		MessageHeaders headers = kafkaMessage.getHeaders();
		kafkaService.ackIfNotYetLogOtherwise(payload.getMsgNum(), payload.getProducer(), payload.getClass().getSimpleName());
		try {
			Thread.sleep(waittime);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
		return null;
	};

	Function<org.springframework.messaging.Message<Mms>, Mono<Void>> mmsListener = kafkaMessage -> {
		Mms payload = kafkaMessage.getPayload();
		MessageHeaders headers = kafkaMessage.getHeaders();
		kafkaService.ackIfNotYetLogOtherwise(payload.getMsgNum(), payload.getProducer(), payload.getClass().getSimpleName());
		try {
			Thread.sleep(waittime);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
		return null;
	};

	Function<org.springframework.messaging.Message<Sms>, Mono<Void>> smsListener = kafkaMessage -> {
		Sms payload = kafkaMessage.getPayload();
		MessageHeaders headers = kafkaMessage.getHeaders();
		kafkaService.ackIfNotYetLogOtherwise(payload.getMsgNum(), payload.getProducer(), payload.getClass().getSimpleName());
		try {
			Thread.sleep(waittime);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
		return null;
	};

	@EventListener(ApplicationStartedEvent.class)
	public void onMessages() {
		// Spring call ApplicationStartedEvent twice, first for the class, second for its proxy.
		if (started) return;
		else started = true;
		mailDispatcher.listen(mailListener);
		messageDispatcher.listen(messageListener);
		mmsDispatcher.listen(mmsListener);
		smsDispatcher.listen(smsListener);
		log.info("All listeners up");
	}

}
