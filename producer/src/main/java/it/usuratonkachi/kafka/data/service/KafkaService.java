package it.usuratonkachi.kafka.data.service;

import it.usuratonkachi.kafka.data.entity.Kafka;
import it.usuratonkachi.kafka.data.repository.KafkaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaService {

	@Value("${spring.profiles:default}")
	private String profile;

	private final KafkaRepository kafkaRepository;

	@Transactional
	@Modifying
	public void createRecordJpa(String msgid, String type, String producer){
		Kafka k = Kafka.builder()
				.id(UUID.randomUUID().toString())
				.msgid(msgid)
				.msgtype(type)
				.producerid(producer)
				.ackreceived(0)
				.build();
		kafkaRepository.saveAndFlush(k);
	}

	@Transactional
	@Modifying
	public void ackIfNotYetLogOtherwise(String msgid, String producerid, String msgtype){
		Kafka k = kafkaRepository.findByMsgidAndMsgtypeAndProducerid(msgid, msgtype, producerid).get();
		if (StringUtils.isEmpty(k.getAckedby())){
			k.setAckedby(profile);
			k = kafkaRepository.saveAndFlush(k);
			log.info("First Received " + k.toString() );
		} else {
			log.warn("Msg already acked! " + k.toString());
			kafkaRepository.addAck(k.getId());
		}
	}

}
