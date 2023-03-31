package com.example.kafka.regular.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class KafkaRegularConsumerApplication {

    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaRegularConsumerApplication.class, args);
        ConsumerService consumerService = context.getBean(ConsumerService.class);
        consumerService.startConsuming();
    }

}
