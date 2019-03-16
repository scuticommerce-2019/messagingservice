package com.scuticommerce.messageservice.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

@RestController
@RequestMapping(value="/api/message/")
public class KafkaMessageController {

    public static final String PRODUCTQUEUE = "productqueue";

    @Autowired
    KafkaTemplate template;

    CountDownLatch latch = new CountDownLatch(3);

    @PostMapping(value="/productqueue")
    public ResponseEntity<?> sendMessage(@RequestBody String message){

        template.send(PRODUCTQUEUE,message);

        return new ResponseEntity<>("OK", HttpStatus.OK);

    }

    @KafkaListener(topics = PRODUCTQUEUE )
    public void listen(ConsumerRecord<?,?> record){

        String message = record.toString();

        System.out.println(message);

        System.out.println("Product is "+record.value());

        latch.countDown();
    }

}
