package com.example.nightwatch;

import com.example.nightwatch.controller.NightwatchControllerService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@ConfigurationPropertiesScan
public class NightwatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(NightwatchApplication.class, args);
    }

    @Bean
    CommandLineRunner runController(NightwatchControllerService controllerService) {
        return args -> controllerService.run().block();
    }
}
