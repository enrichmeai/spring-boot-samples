package com.enrichmeai.samples.auditing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AuditingApplication {

    public static void main(String[] args) {
        SpringApplication.run(AuditingApplication.class, args);
    }
}
