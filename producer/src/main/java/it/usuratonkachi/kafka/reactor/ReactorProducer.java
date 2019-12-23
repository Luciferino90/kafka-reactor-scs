package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.data.service.KafkaService;
import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactiveStreamDispatcher;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderResult;

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

	private final ReactiveStreamDispatcher<Mail> mailDispatcher;
	private final ReactiveStreamDispatcher<Message> messageDispatcher;
	private final ReactiveStreamDispatcher<Mms> mmsDispatcher;
	private final ReactiveStreamDispatcher<Sms> smsDispatcher;

	private final KafkaService kafkaService;

	@Override
	public void run(String... args) {
		//runLimited();
		runForever();
	}

	AtomicInteger base = new AtomicInteger(0);

	private Flux<SenderResult<Object>> sendMail(Integer count, Integer baseCount){
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
				.doOnNext(r ->  System.out.println("Payload: " + r.getPayload() + " Headers: " + r.getHeaders()))
				.flatMap(mailDispatcher::send);
	}

	private Flux<SenderResult<Object>> sendMessage(Integer count, Integer baseCount){
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
				.doOnNext(r ->  System.out.println("Payload: " + r.getPayload() + " Headers: " + r.getHeaders()))
				.flatMap(messageDispatcher::send);
	}

	private Flux<SenderResult<Object>> sendMms(Integer count, Integer baseCount){
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
				.doOnNext(r ->  System.out.println("Payload: " + r.getPayload() + " Headers: " + r.getHeaders()))
				.flatMap(mmsDispatcher::send);
	}

	private Flux<SenderResult<Object>> sendSms(Integer count, Integer baseCount){
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
				.doOnNext(r ->  System.out.println("Payload: " + r.getPayload() + " Headers: " + r.getHeaders()))
				.flatMap(smsDispatcher::send);
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
					//sendMessage(count, basecount).subscribe();
					//sendMms(count, basecount).subscribe();
					//sendSms(count, basecount).subscribe();
				})
				.collectList()
				.block();
	}

}
