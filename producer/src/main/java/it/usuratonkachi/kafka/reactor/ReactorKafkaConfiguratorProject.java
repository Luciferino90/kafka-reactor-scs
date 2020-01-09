package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactorKafkaConfigurator;
import it.usuratonkachi.kafka.reactor.config.ReactorStreamDispatcher;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import static it.usuratonkachi.kafka.spring.streamconfig.Streams.*;

@Service
public class ReactorKafkaConfiguratorProject extends ReactorKafkaConfigurator {

	@Bean ReactorStreamDispatcher<Mail> mailDispatcher(){
		return new ReactorStreamDispatcher<>(Mail.class, reactorKafkaProperties, MAIL_CHANNEL_OUTPUT);
	}

	@Bean ReactorStreamDispatcher<Message> messageDispatcher(){
		return new ReactorStreamDispatcher<>(Message.class, reactorKafkaProperties, MESSAGE_CHANNEL_OUTPUT);
	}

	@Bean ReactorStreamDispatcher<Mms> mmsDispatcher(){
		return new ReactorStreamDispatcher<>(Mms.class, reactorKafkaProperties, MMS_CHANNEL_OUTPUT);
	}

	@Bean ReactorStreamDispatcher<Sms> smsDispatcher(){
		return new ReactorStreamDispatcher<>(Sms.class, reactorKafkaProperties, SMS_CHANNEL_OUTPUT);
	}

}
