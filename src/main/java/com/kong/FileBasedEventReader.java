package com.kong;

import org.opensearch.common.inject.Singleton;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

@Singleton
public class FileBasedEventReader implements IEventReader{

    private int currentLine;
    private final ReentrantLock lock = new ReentrantLock();
    private final List<String> lines;

    public FileBasedEventReader()  {
        try {
            Path path = Paths.get(getClass().getResource("/stream.jsonl").toURI());
            lines = Files.readAllLines(path);
            this.currentLine = 0;
        } catch (IOException | URISyntaxException e) {
            // Handle or log the exceptions appropriately
            throw new RuntimeException("Error initializing FileBasedEventReader", e);
        }
    }

    @Override
    public String getNextEvent() {
        lock.lock();
        try {
            if (currentLine < lines.size()) {
                return lines.get(currentLine++);
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }
}
