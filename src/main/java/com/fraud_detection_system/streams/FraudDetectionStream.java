package com.fraud_detection_system.streams;


import com.fraud_detection_system.events.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Configuration
@EnableKafkaStreams
@Slf4j
public class FraudDetectionStream {


    @Bean
    public KStream<String,String>  fraudStream(StreamsBuilder builder){


       KStream<String,String> transactionStream =  builder.stream("transcation");

       KStream<String,String> fraudTransactionStream = transactionStream
                                        .filter((key,value)-> isSuspicious(value))
                                         .peek((key,value)-> log.warn("FRAUD ALERT ‼️ - transaction={} , value={}",key,value));


        fraudTransactionStream.to("fraud-alerts");

       return transactionStream;

    }


    private boolean isSuspicious(String value){

        try{
            Transaction transaction =  new ObjectMapper().readValue(value, Transaction.class);
            return transaction.amount() > 10000;


        } catch (JacksonException e) {
            return false;
        }
    }



}
