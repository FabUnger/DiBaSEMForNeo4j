package de.evpathfinder.data;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class VisitedNodeId {

    private static final AtomicInteger counter = new AtomicInteger(new Random().nextInt(1000));

    private final String name;
    private final double version;

    public VisitedNodeId(String id) {
        this.name = id;
        this.version = counter.incrementAndGet();
    }

    public String getName() {
        return this.name;
    }

    public double getVersion() {
        return this.version;
    }
}
