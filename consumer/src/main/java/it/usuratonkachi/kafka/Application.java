package it.usuratonkachi.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;


@SpringBootApplication(scanBasePackages = "it.usuratonkachi.kafka.config")
@EnableJpaRepositories(basePackages = { "it.usuratonkachi.kafka.data" })
public class Application {

	public static void main(String[] args) {
		System.setProperty("com.sun.xml.bind.v2.bytecode.ClassTailor.noOptimize", "true");
		SpringApplication.run(Application.class, args);
	}

}
