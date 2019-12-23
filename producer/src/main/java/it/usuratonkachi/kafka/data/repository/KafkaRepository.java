package it.usuratonkachi.kafka.data.repository;

import it.usuratonkachi.kafka.data.entity.Kafka;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface KafkaRepository extends JpaRepository<Kafka, String> {

	Optional<Kafka> findByMsgidAndMsgtypeAndProducerid(String id, String type, String producer);

	@Query(nativeQuery = true, value = "UPDATE kafkacheck SET ackreceived = SUM(ackreceived, 1) WHERE id = :id")
	int addAck(String id);
}
