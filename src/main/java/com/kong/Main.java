package com.kong;

import java.util.Locale;

public class Main {
    public static void main(String[] args) throws Exception {

        String mode = args[0];

        switch(mode.toLowerCase(Locale.ROOT)) {
            case "producer":
                System.out.println("Starting the Producer\n");
                new KafkaEventProducer(new FileBasedEventReader()).runAlways(null);
                break;

            case "consumer":
                System.out.println("Starting the Consumer\n");
                new KafkaEventConsumer().runAlways(new OpenSearchBatchMessageProcessorImpl());
                break;
        }
    }
}