package com.fraud_detection_system.events;


public record Transaction(
        String transactionId,
        String userId,
        double amount,
        String timestamp
) {
}