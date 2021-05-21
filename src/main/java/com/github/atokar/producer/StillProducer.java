package com.github.atokar.producer;

import com.github.atokar.serializer.JsonSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class StillProducer {

    private static final Logger log = LoggerFactory.getLogger(StillProducer.class);

    public final String TOPIC = "still";

    public void runSequence() {
        Producer<Long, Map<String, List<String>>> producer = createProducer();
        Map<String, List<String>> part1 = Collections.singletonMap("notes", Arrays.asList("C4", "E4", "A4"));
        Map<String, List<String>> part2 = Collections.singletonMap("notes", Arrays.asList("B3", "E4", "A4"));
        Map<String, List<String>> part3 = Collections.singletonMap("notes", Arrays.asList("B3", "E4", "G4"));

        try {
            long l = 0;
            while (!Thread.currentThread().isInterrupted()) {
                l++;
                List<CompletableFuture<RecordMetadata>> all = new ArrayList<>(16);
                for (int i = 0; i < 8; i++) {
                    all.add(sendAsync(producer, part1));
                    Thread.sleep(312, 500_000);
                }
                for (int i = 0; i < 3; i++) {
                    all.add(sendAsync(producer, part2));
                   Thread.sleep(312, 500_000);
                }
                for (int i = 0; i < 5; i++) {
                    all.add(sendAsync(producer, part3));
                    Thread.sleep(312, 500_000);
                }
                CompletableFuture.allOf(all.toArray(new CompletableFuture[16]));
                log.debug("Iteration completed {} times", l);
            }
        } catch (InterruptedException ignore) {
            log.info("Sequence was terminated by InterruptedException.");
        } catch (Exception e) {
            log.error("Sequence was terminated general Exception.", e);
        }
    }

    private CompletableFuture<RecordMetadata> sendAsync(Producer<Long, Map<String, List<String>>> producer, Map<String, List<String>> message) {
        ProducerRecord<Long, Map<String, List<String>>> record = new ProducerRecord<>(TOPIC, message);
        CompletableFuture<RecordMetadata> result = new CompletableFuture<>();
        producer.send(record, (metadata, exception) -> {
            if (null != exception) {
                result.completeExceptionally(exception);
            } else {
                result.complete(metadata);
            }
        });
        return result;
    }

    public static Producer<Long, Map<String, List<String>>> createProducer() {
        Properties props = new Properties();
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "still-client");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
