package de.evpathfinder.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PriorityQueue {
    private final Map<VisitedNodeId, Double> nodeMap = new HashMap<>();

    public void put(VisitedNodeId nodeId, double priority) {
        this.nodeMap.put(nodeId, priority);
    }

    public VisitedNodeId poll() {
        Map.Entry<VisitedNodeId, Double> highestPriorityEntry = Collections.min(this.nodeMap.entrySet(), Map.Entry.comparingByValue());
        this.nodeMap.remove(highestPriorityEntry.getKey());
        return highestPriorityEntry.getKey();
    }

    public boolean containsNodeId(String nodeId) {
        for (VisitedNodeId id : nodeMap.keySet()) {
            if (id.equals(nodeId)) {
                return true;
            }
        }
        return false;
    }

    public boolean isEmpty() {
        return this.nodeMap.isEmpty();
    }

}
