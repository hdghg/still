package com.github.atokar.producer;

import com.github.atokar.serializer.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class StillProducer {

    private final String TOPIC = "still";

    public void runSequence() {
        Producer<Long, Map<String, Object>> producer = createProducer();
        Map<String, Object> part1 = Collections.singletonMap("notes", Arrays.asList("C4", "E4", "A4"));
        Map<String, Object> part2 = Collections.singletonMap("notes", Arrays.asList("B3", "E4", "A4"));
        Map<String, Object> part3 = Collections.singletonMap("notes", Arrays.asList("B3", "E4", "G4"));

        ProducerRecord<Long, Map<String, Object>> record;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                for (int i = 0; i < 8; i++) {
                    record = new ProducerRecord<>(TOPIC, part1);
                    producer.send(record);
                    Thread.sleep(312, 500_000);
                }
                for (int i = 0; i < 3; i++) {
                    record = new ProducerRecord<>(TOPIC, part2);
                    producer.send(record);
                    Thread.sleep(312, 500_000);
                }
                for (int i = 0; i < 5; i++) {
                    record = new ProducerRecord<>(TOPIC, part3);
                    producer.send(record);
                    Thread.sleep(312, 500_000);
                }
            }
        } catch (InterruptedException ignore) {
            System.out.println("Sequence was terminated by InterruptedException.");
        }
    }

    public static Producer<Long, Map<String, Object>> createProducer() {
        Properties props = new Properties();
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "still-client");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
