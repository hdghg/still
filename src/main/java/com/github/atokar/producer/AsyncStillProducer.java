package com.github.atokar.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.CircularIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class AsyncStillProducer {

    private static final Logger log = LoggerFactory.getLogger(AsyncStillProducer.class);
    private final Iterator<Map<String, List<String>>> source;
    private final Producer<Long, Map<String, List<String>>> producer = StillProducer.createProducer();

    {
        ArrayList<Map<String, List<String>>> list = new ArrayList<>();
        Stream.generate(() -> Collections.singletonMap("notes", Arrays.asList("C4", "E4", "A4")))
                .limit(8).forEach(list::add);
        Stream.generate(() -> Collections.singletonMap("notes", Arrays.asList("B3", "E4", "A4")))
                .limit(3).forEach(list::add);
        Stream.generate(() -> Collections.singletonMap("notes", Arrays.asList("B3", "E4", "G4")))
                .limit(5).forEach(list::add);
        source = new CircularIterator<>(list);
    }


    public CompletableFuture<Void> runSequenceAsync() {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        executorService.scheduleAtFixedRate(this::processNext, 0, 312_500_000, TimeUnit.NANOSECONDS);

        CompletableFuture<Void> result = new CompletableFuture<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook called");
            executorService.shutdownNow();
            result.complete(null);
        }));
        return result;
    };

    private void processNext() {
        log.debug("Sending message");
        Map<String, List<String>> next = source.next();
        ProducerRecord<Long, Map<String, List<String>>> record = new ProducerRecord<>("still", next);
        producer.send(record);
    }
}
