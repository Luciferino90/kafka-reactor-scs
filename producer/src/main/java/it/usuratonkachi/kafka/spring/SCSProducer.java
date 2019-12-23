package it.usuratonkachi.kafka.spring;

import it.usuratonkachi.kafka.data.service.KafkaService;
import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.spring.streamconfig.Streams;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@RequiredArgsConstructor
@Component
@Slf4j
public class SCSProducer implements CommandLineRunner {

	@Value("${spring.profiles:default}")
	private String profile;

	private final Integer count;
	private final Long waittime;

	private final KafkaService kafkaService;

	AtomicInteger base = new AtomicInteger(0);

	@Autowired
	@Qualifier(Streams.MAIL_CHANNEL_OUTPUT)
	private MessageChannel mailChannelOutput;
	@Autowired
	@Qualifier(Streams.MESSAGE_CHANNEL_OUTPUT)
	private MessageChannel messageChannelOutput;
	@Autowired
	@Qualifier(Streams.MMS_CHANNEL_OUTPUT)
	private MessageChannel mmsChannelOutput;
	@Autowired
	@Qualifier(Streams.SMS_CHANNEL_OUTPUT)
	private MessageChannel smsChannelOutput;

	@Override
	public void run(String... args) {
		//runLimited();
		runForever();
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

	private Flux<Boolean> sendMail(Integer count, Integer baseCount){
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
				.map(r -> mailChannelOutput.send(MessageBuilder.withPayload(r).build()));
	}

		/*private Flux<Boolean> sendMessage(Integer count, Integer baseCount){
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
				.map(r -> messageChannelOutput.send(MessageBuilder.withPayload(r).build()));
	}

	private Flux<Boolean> sendMms(Integer count, Integer baseCount){
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
				.map(r -> mmsChannelOutput.send(MessageBuilder.withPayload(r).build()));
	}

	private Flux<Boolean> sendSms(Integer count, Integer baseCount){
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
				.map(r -> smsChannelOutput.send(MessageBuilder.withPayload(r).build()));
	}*/

}
