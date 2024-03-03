package com.kong;

import org.apache.log4j.Logger;

/**
 * The type Abstract class Kafka
 */
public abstract class AbstractKafka {

    private final Logger log = Logger.getLogger(AbstractKafka.class.getName());

    /**
     * Instantiates a new Abstract class.
     * <p>
     * This abstract class's constructor provides graceful
     * shutdown behavior for Kafka producers and consumers
     */
    public AbstractKafka() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    /**
     * The inherited classes will provide the behavior necessary
     * to shut down gracefully.
     *
     * @throws Exception the exception that get thrown upon error
     */
    public abstract void shutdown() throws Exception;

    /**
     * This purpose of this method is to provide continuous
     * behavior to produce or consume messages from a Kafka
     * broker
     *
     * @param callback a callback function to provide processing
     *                 logic after a message is produced or after
     *                 a message is consumed
     * @throws Exception the exception that get thrown upon error
     */
    public abstract void runAlways(OpenSearchBatchMessageProcessorImpl callback) throws Exception;

}
