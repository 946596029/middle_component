package org.example;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.example.producer.KafkaProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Random;

@SpringBootApplication
public class Main {
    @Resource
    private KafkaProducer kafkaProducer;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @PostConstruct
    public void test() throws InterruptedException {
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        kafkaProducer.sendSyncMessage(new Random().nextLong(), "hello world");
        kafkaProducer.sendSyncMessage(new Random().nextLong(), "hello world2");
        for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            kafkaProducer.sendSyncMessage(new Random().nextLong(), "message" + i);
        }
    }
}