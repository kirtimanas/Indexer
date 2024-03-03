package com.kong;

import org.apache.log4j.Logger;

public abstract class AbstractKafkaSimple {

    private final Logger log = Logger.getLogger(AbstractKafkaSimple.class.getName());

    public AbstractKafkaSimple() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    public abstract void shutdown() throws Exception;

    public abstract void runAlways(OpenSearchBatchMessageProcessorImpl callback) throws Exception;

}
