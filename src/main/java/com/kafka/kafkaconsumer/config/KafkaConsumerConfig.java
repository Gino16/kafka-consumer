package com.kafka.kafkaconsumer.config;

import com.kafka.kafkaconsumer.model.KafkaConfigData;
import com.kafka.kafkaconsumer.model.KafkaConsumerConfigData;
import java.io.Serializable;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

@Configuration
@RequiredArgsConstructor
@EnableKafka
public class KafkaConsumerConfig<K extends Serializable, V extends Serializable> {
  private final KafkaConfigData kafkaConfigData;
  private final KafkaConsumerConfigData kafkaConsumerConfigData;

  @Bean
  public Map<String, Object> consumerConfigs() {
    return Map.ofEntries(
        Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigData.getBootstrapServers()),
        Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            kafkaConsumerConfigData.getKeyDeserializer()),
        Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            kafkaConsumerConfigData.getValueDeserializer()),
        Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            kafkaConsumerConfigData.getAutoOffsetReset()),
        Map.entry(kafkaConfigData.getSchemaRegistryUrlKey(),
            kafkaConfigData.getSchemaRegistryUrl()),
        Map.entry(kafkaConsumerConfigData.getSpecificAvroReaderKey(),
            kafkaConsumerConfigData.getSpecificAvroReader()),
        Map.entry(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
            kafkaConsumerConfigData.getSessionTimeoutMs()),
        Map.entry(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
            kafkaConsumerConfigData.getHeartbeatIntervalMs()),
        Map.entry(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
            kafkaConsumerConfigData.getMaxPollIntervalMs()),
        Map.entry(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
            kafkaConsumerConfigData.getMaxPartitionFetchBytesDefault() *
                kafkaConsumerConfigData.getMaxPartitionFetchBytesBoostFactor()),
        Map.entry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
            kafkaConsumerConfigData.getMaxPollRecords())
    );
  }

  @Bean
  public ConsumerFactory<K, V> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
  }

  @Bean
  public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<K, V> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.setBatchListener(kafkaConsumerConfigData.getBatchListener());
    factory.setConcurrency(kafkaConsumerConfigData.getConcurrencyLevel());
    factory.setAutoStartup(kafkaConsumerConfigData.getAutoStartup());
    factory.getContainerProperties().setPollTimeout(kafkaConsumerConfigData.getPollTimeoutMs());
    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
    return factory;
  }
}
