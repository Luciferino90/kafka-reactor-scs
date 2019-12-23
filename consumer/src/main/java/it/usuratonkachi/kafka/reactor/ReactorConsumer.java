package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.data.service.KafkaService;
import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactiveStreamDispatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class ReactorConsumer {

	@Value("${spring.profiles:default}")
	private String profile;

	private static Boolean started = false;

	private final ReactiveStreamDispatcher<Mail> mailDispatcher;
	private final ReactiveStreamDispatcher<Message> messageDispatcher;
	private final ReactiveStreamDispatcher<Mms> mmsDispatcher;
	private final ReactiveStreamDispatcher<Sms> smsDispatcher;

	private final KafkaService kafkaService;

	@EventListener(ApplicationStartedEvent.class)
	public void onMessages() {
		if (started) return;
		else started = true;
		long waitTime = 0L;
		mailDispatcher.listen()
				.doOnNext(r -> {
					kafkaService.ackIfNotYetLogOtherwise(r.getPayload().getMsgNum(), r.getPayload().getProducer(), r.getPayload().getClass().getSimpleName());
					try {
						Thread.sleep(waitTime);
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				})
				.collectList()
				.subscribe();
		messageDispatcher.listen()
				.doOnNext(r -> {
					kafkaService.ackIfNotYetLogOtherwise(r.getPayload().getMsgNum(), r.getPayload().getProducer(), r.getPayload().getClass().getSimpleName());
					try {
						Thread.sleep(waitTime);
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				})
				.subscribe();
		mmsDispatcher.listen()
				.doOnNext(r -> {
					kafkaService.ackIfNotYetLogOtherwise(r.getPayload().getMsgNum(), r.getPayload().getProducer(), r.getPayload().getClass().getSimpleName());
					try {
						Thread.sleep(waitTime);
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				})
				.subscribe();
		smsDispatcher.listen()
				.doOnNext(r -> {
					kafkaService.ackIfNotYetLogOtherwise(r.getPayload().getMsgNum(), r.getPayload().getProducer(), r.getPayload().getClass().getSimpleName());
					try {
						Thread.sleep(waitTime);
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				})
				.subscribe();
		log.info("All listeners up");
	}

}
