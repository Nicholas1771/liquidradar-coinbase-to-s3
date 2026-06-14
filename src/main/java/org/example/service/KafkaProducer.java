package org.example.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducer {

    private final BlockingQueue<String> queue;
    private final ExecutorService executor;
    private final Config.KafkaConfig kafkaConfig;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @PostConstruct
    public void consume() {
        executor.submit(() -> {
            log.info("KafkaProducer thread started, waiting for data...");
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String tradeData = queue.take();
                    log.info("Sending to kafka: {}", tradeData);
                    kafkaTemplate.send(kafkaConfig.getTopic(), tradeData)
                            .whenComplete((result, ex) -> {
                                if (ex != null) {
                                    // This executes if Kafka rejects the message or times out
                                    log.error("ERROR sending record to Kafka topic {}", tradeData, ex);
                                } else {
                                    log.debug("Successfully delivered to topic {} partition {}",
                                            result.getRecordMetadata().topic(),
                                            result.getRecordMetadata().partition());
                                }
                            });
                }
            } catch (InterruptedException e) {
                log.info("Consumer thread interrupted, shutting down smoothly.");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error in consumer loop", e);
            }
        });
    }

}
