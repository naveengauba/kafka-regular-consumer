package com.example.kafka.regular.consumer;

import com.sun.management.OperatingSystemMXBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class KafkaRegularConsumerApplication implements DisposableBean {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private static final String GROUP_ID = "regular-consumer";

    public static final String SAMPLE_TOPIC = "sample-topic";

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");

    private static KafkaConsumer<String, String> consumer;

    private static final AtomicInteger messagesConsumed = new AtomicInteger();

    public static void main(String[] args) throws IOException, InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaRegularConsumerApplication.class, args);

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // create consumer
        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(SAMPLE_TOPIC));
        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                messagesConsumed.incrementAndGet();
                //log.info("Key: " + record.key() + ", Value:" + record.value());
                //log.info("Partition:" + record.partition() + ",Offset:" + record.offset());
            }
        }
    }

    @Scheduled(initialDelay = 15000, fixedDelay = 15000)
    public void printStats() {
        OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
                OperatingSystemMXBean.class);
        // % CPU load this current JVM is taking, from 0.0-1.0
        double processCpuLoad = osBean.getProcessCpuLoad() * 100;

        // % CPU load the overall system is at, from 0.0-1.0
        double systemCpuLoad = osBean.getCpuLoad() * 100;
        log.info("Timestamp/ProcessCpuLoad/SystemCpuLoad/messagesConsumed: \n{}, {}, {}, {}", DATE_FORMAT.format(new Date()), DECIMAL_FORMAT.format(processCpuLoad), DECIMAL_FORMAT.format(systemCpuLoad), messagesConsumed.get());
    }

    @Override
    public void destroy() throws Exception {
        log.info("Shutting down KafkaRegularConsumerApplication");
        consumer.close();
    }
}
