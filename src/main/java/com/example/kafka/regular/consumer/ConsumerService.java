package com.example.kafka.regular.consumer;

import com.sun.management.OperatingSystemMXBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class ConsumerService {

    @Value(value = "${dummy.topic.name}")
    private String sampleTopic;

    @Value(value = "${oms.topic.name}")
    private String omsTopic;

    @Value("${process.sample.topic}")
    private boolean processSampleTopic;

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");

    private static final AtomicInteger messagesConsumed = new AtomicInteger();

    private final KafkaConsumer<String, String> consumer;

    private final KafkaConsumer<String, Object> omsConsumer;

    public ConsumerService(KafkaConsumer<String, String> consumer, KafkaConsumer<String, Object> omsConsumer) {
        this.consumer = consumer;
        this.omsConsumer = omsConsumer;
    }

    public void startConsuming() {

        if (processSampleTopic) {
            consumer.subscribe(Collections.singletonList(sampleTopic));
            log.info("Consuming messages from sample-topic");
            while (true) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    messagesConsumed.incrementAndGet();
                }
            }
        } else {
            omsConsumer.subscribe(Collections.singletonList(omsTopic));
            log.info("Consuming messages from oms-topic");
            while (true) {
                final ConsumerRecords<String, Object> consumerRecords = omsConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Object> record : consumerRecords) {
                    messagesConsumed.incrementAndGet();
                }
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
        log.info("Timestamp/ProcessCpuLoad/SystemCpuLoad/messagesConsumed: \n{}, {}, {}, {}",
                DATE_FORMAT.format(new Date()),
                DECIMAL_FORMAT.format(processCpuLoad),
                DECIMAL_FORMAT.format(systemCpuLoad), messagesConsumed.get());
    }
}
