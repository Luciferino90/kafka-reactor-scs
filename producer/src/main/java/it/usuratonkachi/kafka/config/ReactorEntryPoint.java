package it.usuratonkachi.kafka.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.annotation.PostConstruct;

@Configuration
@Profile("reactive")
@ComponentScan(basePackages = { "it.usuratonkachi.kafka.reactor", "it.usuratonkachi.kafka.data" })
public class ReactorEntryPoint {
}
