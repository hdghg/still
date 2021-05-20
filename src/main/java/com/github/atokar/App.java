package com.github.atokar;

import com.github.atokar.producer.StillProducer;

public class App {

    public static void main(String[] args) {
        StillProducer stillProducer = new StillProducer();
        stillProducer.runSequence();
    }
}
