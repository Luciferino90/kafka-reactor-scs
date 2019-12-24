package it.usuratonkachi.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Mail implements Serializable {

	private String className = this.getClass().getSimpleName();
	private String producer;
	private String msgNum;

}
