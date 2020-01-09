package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.data.service.KafkaService;
import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactorStreamDispatcher;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Component
@RequiredArgsConstructor
public class ReactorProducer implements CommandLineRunner {

	@Value("${spring.profiles}")
	private String profile;

	@Value("${default.count:10}")
	private Integer count;
	@Value("${default.waittime:1000}")
	private Long waittime;

	private final ReactorStreamDispatcher<Mail> mailDispatcher;
	private final ReactorStreamDispatcher<Message> messageDispatcher;
	private final ReactorStreamDispatcher<Mms> mmsDispatcher;
	private final ReactorStreamDispatcher<Sms> smsDispatcher;

	private final KafkaService kafkaService;

	@Override
	public void run(String... args) {
		count = 1000;
		// runLimited();
		runForever();
	}

	AtomicInteger base = new AtomicInteger(0);

	private Flux<Disposable> sendMail(Integer count, Integer baseCount){
		int start = baseCount;
		int end = count + baseCount;
		return Flux.fromStream(IntStream.range(start, end).boxed())
				.map(j -> {
					Mail mail = new Mail();
					mail.setMsgNum(UUID.randomUUID().toString());
					mail.setProducer(profile);
					return mail;
				})
				.doOnNext(_msg -> kafkaService.createRecordJpa(_msg.getMsgNum(), _msg.getClass().getSimpleName(), _msg.getProducer()))
				.map(o -> MessageBuilder.withPayload(o)
						.copyHeaders(Map.of("X-Test", "Prova MAIL"))
						.build()
				)
				//.doOnNext(r ->  System.out.println("Payload: " + r.getPayload() + " Headers: " + r.getHeaders()))
				.map(mailDispatcher::send);
	}

	private Flux<Disposable> sendMessage(Integer count, Integer baseCount){
		int start = baseCount;
		int end = count + baseCount;
		return Flux.fromStream(IntStream.range(start, end).boxed())
				.map(j -> {
					Message message = new Message();
					message.setMsgNum(UUID.randomUUID().toString());
					message.setProducer(profile);
					return message;
				})
				.doOnNext(_msg -> kafkaService.createRecordJpa(_msg.getMsgNum(), _msg.getClass().getSimpleName(), _msg.getProducer()))
				.map(o -> MessageBuilder.withPayload(o)
						.copyHeaders(Map.of("X-Test", "Prova MAIL"))
						.build()
				)
				//.doOnNext(r ->  System.out.println("Payload: " + r.getPayload() + " Headers: " + r.getHeaders()))
				.map(messageDispatcher::send);
	}

	private Flux<Disposable> sendMms(Integer count, Integer baseCount){
		int start = baseCount;
		int end = count + baseCount;
		return Flux.fromStream(IntStream.range(start, end).boxed())
				.map(j -> {
					Mms mms = new Mms();
					mms.setMsgNum(UUID.randomUUID().toString());
					mms.setProducer(profile);
					return mms;
				})
				.doOnNext(_msg -> kafkaService.createRecordJpa(_msg.getMsgNum(), _msg.getClass().getSimpleName(), _msg.getProducer()))
				.map(o -> MessageBuilder.withPayload(o)
						.copyHeaders(Map.of("X-Test", "Prova MAIL"))
						.build()
				)
				//.doOnNext(r ->  System.out.println("Payload: " + r.getPayload() + " Headers: " + r.getHeaders()))
				.map(mmsDispatcher::send);
	}

	private Flux<Disposable> sendSms(Integer count, Integer baseCount){
		int start = baseCount;
		int end = count + baseCount;
		return Flux.fromStream(IntStream.range(start, end).boxed())
				.map(j -> {
					Sms sms = new Sms();
					sms.setMsgNum(UUID.randomUUID().toString());
					sms.setProducer(profile);
					return sms;
				})
				.doOnNext(_msg -> kafkaService.createRecordJpa(_msg.getMsgNum(), _msg.getClass().getSimpleName(), _msg.getProducer()))
				.map(o -> MessageBuilder.withPayload(o)
						.copyHeaders(Map.of("X-Test", "Prova MAIL"))
						.build()
				)
				//.doOnNext(r ->  System.out.println("Payload: " + r.getPayload() + " Headers: " + r.getHeaders()))
				.map(smsDispatcher::send);
	}

	public void runLimited() {
		String msg = profile;
		sendMail(count, 0).subscribe();
		//sendMessage(count, 0).subscribe();
		//sendMms(count, 0).subscribe();
		//sendSms(count, 0).collectList().block();
	}

	public void runForever() {
		String msg = profile;
		Flux.interval(Duration.ofMillis(waittime))
				.doOnNext(i -> {
					int basecount = base.getAndIncrement();
					sendMail(count, basecount).subscribe();
					sendMessage(count, basecount).subscribe();
					sendMms(count, basecount).subscribe();
					sendSms(count, basecount).subscribe();
				})
				.collectList()
				.block();
	}

}
