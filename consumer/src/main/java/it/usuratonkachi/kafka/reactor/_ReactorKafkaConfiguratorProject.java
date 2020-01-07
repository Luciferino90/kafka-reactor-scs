package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactorKafkaConfigurator;
import it.usuratonkachi.kafka.reactor.config._ReactorStreamDispatcher;
import it.usuratonkachi.kafka.reactor.streamconfig.Streams;
import org.springframework.context.annotation.Bean;

//@Service
@Deprecated // Use ByteArray version
public class _ReactorKafkaConfiguratorProject extends ReactorKafkaConfigurator {

	@Bean _ReactorStreamDispatcher<Mail> mailDispatcher(){
		return new _ReactorStreamDispatcher<>(reactorKafkaProperties, Streams.MAIL_CHANNEL_INPUT);
	}

	@Bean _ReactorStreamDispatcher<Message> messageDispatcher(){
		return new _ReactorStreamDispatcher<>(reactorKafkaProperties, Streams.MESSAGE_CHANNEL_INPUT);
	}

	@Bean _ReactorStreamDispatcher<Mms> mmsDispatcher(){
		return new _ReactorStreamDispatcher<>(reactorKafkaProperties, Streams.MMS_CHANNEL_INPUT);
	}

	@Bean _ReactorStreamDispatcher<Sms> smsDispatcher(){
		return new _ReactorStreamDispatcher<>(reactorKafkaProperties, Streams.SMS_CHANNEL_INPUT);
	}

}
