package com.github.atokar;

import com.github.atokar.producer.AsyncStillProducer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class App {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AsyncStillProducer asyncStillProducer = new AsyncStillProducer();
        CompletableFuture<Void> completableFuture = asyncStillProducer.runSequenceAsync();
        completableFuture.get();
    }
}
