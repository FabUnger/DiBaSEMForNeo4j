package de.evpathfinder.data;

import java.util.ArrayList;
import java.util.List;

public class Path {

    private final List<VisitedNode> path;

    public Path(List<VisitedNode> path){
        this.path = new ArrayList<>(path);
    }

    public List<VisitedNode> getPath() {
        return this.path;
    }

    public double getTravelTimeOfNode(VisitedNodeId id) {
        for (VisitedNode node : this.path) {
            if (node.id().equals(id)) {
                return node.travelTime();
            }
        }
        return Double.MAX_VALUE;
    }

    public double getSocOfNode(VisitedNodeId id) {
        for (VisitedNode node : this.path) {
            if (node.id().equals(id)) {
                return node.soc();
            }
        }
        return -1;
    }

    public double getChargingTimeOfNode(VisitedNodeId id) {
        for (VisitedNode node : this.path) {
            if (node.id().equals(id)) {
                return node.chargingTime();
            }
        }
        return 0.0;
    }

    public VisitedNodeId getParentOfNode(VisitedNodeId id) {
        for (VisitedNode node : this.path) {
            if (node.id().equals(id)) {
                int index = this.path.indexOf(node);
                if (index - 1 < 0) return null;
                return this.path.get(index - 1).id();
            }
        }
        return null;
    }

    public VisitedNodeId getLastStation() {
        for (int i = this.path.size() - 1; i >= 0; i--) {
            VisitedNode node = this.path.get(i);
            if (node.chargingTime() > 0.0) {
                return node.id();
            }
        }
        return null;
    }

    public VisitedNode getLastNode() {
        int index = path.size() - 1;
        return this.path.get(index);
    }

    public VisitedNode getNodeById(VisitedNodeId id) {
        for (VisitedNode node : this.path) {
            if (node.id().equals(id)) {
                return node;
            }
        }
        return null;
    }
}
