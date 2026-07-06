package com.onidza.notificationservice.kafka;

import com.onidza.grpc.client.ClientForNotificationResponse;
import com.onidza.notificationservice.grpc.BackendClientGrpcClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class ConsumerOrderNotification {

    private final ObjectMapper objectMapper;
    private final BackendClientGrpcClient backendClientGrpcClient;

    @KafkaListener(
            topics = "order-notification",
            containerFactory = "kafkaListenerContainerFactory",
            groupId = "notification-service"
    )
    public void consumeNotification(List<ConsumerRecord<String, String>> records) {
        log.info("Orders notifications received, size: {}", records.size());

        for (ConsumerRecord<String, String> consumerRecord : records) {
            try {
                OrderCreateEvent event = objectMapper.readValue(
                        consumerRecord.value(),
                        OrderCreateEvent.class
                );

                ClientForNotificationResponse clientInfo
                        = backendClientGrpcClient.getClientForNotificationResponse(event.clientId());

                log.info(
                        "Notification data prepared: name = {}, email = {}",
                        clientInfo.getName(),
                        clientInfo.getEmail()
                );
            } catch (Exception ex) {
                log.error("Failed to process order notification, value = {}", consumerRecord.value(), ex);

                throw new RuntimeException(ex);
            }
        }
    }
}
