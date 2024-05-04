package de.evpathfinder.data;

public record VisitedNode(VisitedNodeId id, double travelTime, double soc, double chargingTime) {
    public VisitedNode(String id, double travelTime, double soc, double chargingTime) {
        this(new VisitedNodeId(id), travelTime, soc, chargingTime);
    }
}
