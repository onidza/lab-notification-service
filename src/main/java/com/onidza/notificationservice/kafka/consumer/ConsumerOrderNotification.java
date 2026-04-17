package com.onidza.notificationservice.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class ConsumerOrderNotification {

    @KafkaListener(topics = "orders-notification", containerFactory = "kafkaListenerContainerFactory")
    public void consumeNotification(List<ConsumerRecord<String, String>> records) {
        log.info("Orders notifications received, size: {}", records.size());

        for (ConsumerRecord<String, String> consumerRecord : records) {
            log.info(
                    "topic={}, partition={}, offset={}, key={}, value={}",
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.key(),
                    consumerRecord.value()
            );
        }
    }
}
