package it.usuratonkachi.kafka.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("spring")
@ComponentScan(basePackages = { "it.usuratonkachi.kafka.spring", "it.usuratonkachi.kafka.data" })
public class SCSEntryPoint {
}
