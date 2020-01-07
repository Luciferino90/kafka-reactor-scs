package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactorKafkaConfigurator;
import it.usuratonkachi.kafka.reactor.config.ReactorStreamDispatcher;
import it.usuratonkachi.kafka.reactor.streamconfig.Streams;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

@Service
public class ReactorKafkaConfiguratorProject extends ReactorKafkaConfigurator {

	@Bean ReactorStreamDispatcher<Mail> mailDispatcher(){
		return new ReactorStreamDispatcher<>(Mail.class, reactorKafkaProperties, Streams.MAIL_CHANNEL_INPUT);
	}

	@Bean ReactorStreamDispatcher<Message> messageDispatcher(){
		return new ReactorStreamDispatcher<>(Message.class, reactorKafkaProperties, Streams.MESSAGE_CHANNEL_INPUT);
	}

	@Bean ReactorStreamDispatcher<Mms> mmsDispatcher(){
		return new ReactorStreamDispatcher<>(Mms.class, reactorKafkaProperties, Streams.MMS_CHANNEL_INPUT);
	}

	@Bean ReactorStreamDispatcher<Sms> smsDispatcher(){
		return new ReactorStreamDispatcher<>(Sms.class, reactorKafkaProperties, Streams.SMS_CHANNEL_INPUT);
	}

}
