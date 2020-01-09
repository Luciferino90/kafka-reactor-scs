package it.usuratonkachi.kafka.data.repository;

import it.usuratonkachi.kafka.data.entity.Kafka;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface KafkaRepository extends JpaRepository<Kafka, String> {

	Optional<Kafka> findByMsgidAndMsgtypeAndProducerid(String id, String type, String producer);

	@Modifying
	@Query(nativeQuery = true, value = "UPDATE kafka SET ackreceived = ackreceived + 1 where id = :id")
	void addAck(String id);
}
