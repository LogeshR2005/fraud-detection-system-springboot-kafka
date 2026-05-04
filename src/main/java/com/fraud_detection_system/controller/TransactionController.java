package com.fraud_detection_system.controller;

import com.fraud_detection_system.events.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.Random;

@RestController
@RequestMapping("/api/transaction")
public class TransactionController {

     private final KafkaTemplate<String,String> kafkaTemplate;
     private final ObjectMapper mapper = new ObjectMapper();

    public TransactionController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public String sendTransaction(){

        for(int i = 0; i < 50; i++){
            String transactionId = "txn-" +System.currentTimeMillis()+"-"+i;
            double amount = 8000+new Random().nextDouble()*(11000-8000);


            Transaction txn = new Transaction(transactionId,"USER_"+i,amount, LocalDateTime.now().toString());

            try {
                String txnJson = mapper.writeValueAsString(txn);
                kafkaTemplate.send("transcation", transactionId, txnJson);
            } catch (Exception e) {
                return "Serialization failed";
            }


        }

    return "Transaction sent to Kafka ✅";
    }
}
