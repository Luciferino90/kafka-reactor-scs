package it.usuratonkachi.kafka.data.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(indexes = {
		@Index(name = "msgid_idx",  columnList="msgid", unique = false),
		@Index(name = "msgtype_idx", columnList="msgtype",     unique = false),
		@Index(name = "producerid_idx", columnList="producerid",     unique = false),
		@Index(name = "partition_idx", columnList="partitionnumber",     unique = false),
		@Index(name = "offset_idx", columnList="offsetnumber",     unique = false)
})
public class Kafka implements Serializable {

	private Integer partitionnumber;
	private Integer offsetnumber;

	@Id
	private String id;

	private String msgid;
	private String msgtype;
	private String producerid;
	private String ackedby;
	private Integer ackreceived;

}
