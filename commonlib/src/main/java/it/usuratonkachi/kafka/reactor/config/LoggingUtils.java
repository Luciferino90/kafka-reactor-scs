package it.usuratonkachi.kafka.reactor.config;

import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LogLevel.*;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.network.Receive;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@UtilityClass
public class LoggingUtils {

	public static void log(LogLevel logLevel, String message, ReceiverRecord<byte[], byte[]> receiverRecord){
		log(logLevel, message, receiverRecord.topic(), receiverRecord.partition(), receiverRecord.offset());
	}

	public static void log(LogLevel logLevel, String message, String topic, int partition, long offset){
		switch (logLevel){
		case INFO:
			if (log.isInfoEnabled())
				log.info(String.format(message,
						topic,
						partition,
						offset));
			break;
		case DEBUG:
			if (log.isDebugEnabled())
				log.debug(String.format(message,
						topic,
						partition,
						offset));
			break;
		case TRACE:
			if (log.isTraceEnabled())
				log.trace(String.format(message,
						topic,
						partition,
						offset));
			break;
		case WARN:
			if (log.isWarnEnabled())
				log.warn(String.format(message,
						topic,
						partition,
						offset));
			break;
		case ERROR:
			if (log.isErrorEnabled())
				log.error(String.format(message,
						topic,
						partition,
						offset));
			break;
		}
	}

}
