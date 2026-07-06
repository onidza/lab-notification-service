package com.onidza.notificationservice.kafka;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Builder
public record OrderCreateEvent(
        Long clientId,
        LocalDateTime orderDate,
        BigDecimal totalAmount,
        OrderStatus status
) {
}
