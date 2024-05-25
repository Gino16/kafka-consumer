package com.kafka.kafkaconsumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerServiceImpl {

  @KafkaListener(topics = "topic-example", groupId = "group1")
  public void listenMessage(@Payload String message, Acknowledgment acknowledgment)
      throws InterruptedException {
    log.info("Message received: {}", message);
    acknowledgment.acknowledge();
    Thread.sleep(1000);
  }
}
