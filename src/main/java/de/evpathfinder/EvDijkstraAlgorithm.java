package de.evpathfinder;

import de.evpathfinder.data.*;
import de.evpathfinder.data.Path;
import de.evpathfinder.data.PriorityQueue;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

public class EvDijkstraAlgorithm {

    @Context
    public Transaction tx;

    public record NodeContainer(Node node, String id, double chargingPower) {}
    public record EdgeContainer(Relationship relationship, double duration, double consumption, String sourceId, String destinationId) {
        public EdgeContainer(Relationship relationship, double duration, double consumption) {
            this(relationship, duration, consumption, (String) relationship.getStartNode().getProperty("id", ""), (String) relationship.getEndNode().getProperty("id", ""));
        }
    }
    private static Map<VisitedNodeId, Path> pathOfNode;

    @Procedure
    public Stream<VisitedNodeResult> executeEvDijkstra(@Name("startId") String startId, @Name("endId") String endId, @Name("maxSoc") double maxSoc, @Name("initialCharge") double initialCharge, @Name("minChargingTime") double minChargingTime) {
        NodeContainer start = this.getNodeById(startId);
        NodeContainer end = this.getNodeById(endId);

        PriorityQueue queue = new PriorityQueue();
        this.initialize(queue, start, initialCharge);

        Path result = null;

        double steps = 0;

        while (!queue.isEmpty()) {
            steps++;

            Path pathOfU = this.pathOfNode.get(queue.poll());
            VisitedNode u = pathOfU.getLastNode();


            if (u.id().getName().equals(end.id)) {
                // Endknoten gefunden. Dessen Nachbarn muessen nicht mehr ueberprueft werden.
                result = pathOfU;
                break;
            }

            List<EdgeContainer> edgesFromU = this.getEdgesFromSourceNode(u.id().getName());
            for (EdgeContainer edgeFromU : edgesFromU) {
                NodeContainer v = this.getNodeById(edgeFromU.destinationId);

                double duration = edgeFromU.duration;
                double consumption = edgeFromU.consumption;

                double currentTravelTime = pathOfU.getTravelTimeOfNode(u.id()) + duration;
                double currentSoc = pathOfU.getSocOfNode(u.id()) - consumption;

                if (currentSoc < 0) {
                    // Nicht genuegend Energie, um zu v zu gelangen

                    VisitedNodeId lastStationId = pathOfU.getLastStation();

                    double lastStationChargingTime = 0.0;
                    double oldChargingTime = pathOfU.getChargingTimeOfNode(lastStationId);

                    double totalConsumption = 0;
                    boolean lastStationChargedEnough = true;

                    if (lastStationId != null) {
                        if (pathOfU.getSocOfNode(lastStationId) > maxSoc) {
                            lastStationChargedEnough = false;
                        }
                        // Falls letzte Ladestation existiert und noch nicht bis 100 % geladen wurde.
                        VisitedNodeId currentNodeId = u.id();
                        totalConsumption = consumption;
                        while (currentNodeId != null && !currentNodeId.equals(lastStationId)) {
                            VisitedNodeId parentNodeId = pathOfU.getParentOfNode(currentNodeId);
                            if (parentNodeId != null) {
                                totalConsumption += this.getShortestEdgeBetweenNodes(parentNodeId.getName(), currentNodeId.getName()).consumption;
                            }
                            currentNodeId = parentNodeId;
                        }

                        double necessarySoc = totalConsumption;

                        if (necessarySoc > maxSoc) {
                            lastStationChargedEnough = false;
                            oldChargingTime = 0.0;
                        } else {
                            double additionalChargingTime = this.calculateAdditionalChargeTime(pathOfU.getSocOfNode(lastStationId), totalConsumption, this.getNodeById(lastStationId.getName()).chargingPower);
                            lastStationChargingTime = oldChargingTime + additionalChargingTime;
                        }
                    }

                    if (lastStationId == null || !lastStationChargedEnough) {
                        // Suche nach allen Ladestationen von u bis p oder Verbrauch zu groß wird, um Aufladen zu koennen.

                        totalConsumption = consumption;
                        VisitedNodeId currentNodeId = u.id();
                        Map<VisitedNodeId, Double> lastStations = new HashMap<>();
                        while (currentNodeId != null && !currentNodeId.equals(lastStationId)) {
                            if (this.getNodeById(currentNodeId.getName()).chargingPower > 0 && pathOfU.getSocOfNode(currentNodeId) + totalConsumption < maxSoc) {
                                lastStations.put(currentNodeId, totalConsumption);
                            }
                            VisitedNodeId parentNodeId = pathOfU.getParentOfNode(currentNodeId);
                            if (parentNodeId != null) {
                                totalConsumption += this.getShortestEdgeBetweenNodes(parentNodeId.getName(), currentNodeId.getName()).consumption;
                            }
                            currentNodeId = parentNodeId;
                        }

                        double newChargingTimeLastStation = Double.MAX_VALUE;
                        for (Map.Entry<VisitedNodeId, Double> lastStation : lastStations.entrySet()) {
                            VisitedNodeId w = lastStation.getKey();
                            double necessarySoc = lastStation.getValue();
                            if (necessarySoc > maxSoc) {
                                break;
                            }
                            if (pathOfU.getSocOfNode(w) > necessarySoc) {
                                lastStationId = w;
                                break;
                            }
                            double tempNewChargingTimeLastStation = this.calculateAdditionalChargeTime(pathOfU.getSocOfNode(w), necessarySoc, this.getNodeById(w.getName()).chargingPower);
                            if (tempNewChargingTimeLastStation < newChargingTimeLastStation) {
                                newChargingTimeLastStation = tempNewChargingTimeLastStation;
                                lastStationId = w;
                                lastStationChargingTime = newChargingTimeLastStation;
                                totalConsumption = necessarySoc;
                            }
                        }
                    }

                    if (lastStationId == null) {
                        // v kann nicht ueber diesen Weg erreicht werden, da keine Ladestation nah genug dran liegt.
                        continue;
                    }

                    if (minChargingTime > lastStationChargingTime && pathOfU.getNodeById(lastStationId).soc() < totalConsumption) {
                        lastStationChargingTime = minChargingTime;
                    }

                    double newTravelTimeV = currentTravelTime - oldChargingTime + lastStationChargingTime;

                    List<VisitedNode> visitedNodes = new ArrayList<>();

                    List<VisitedNode> visitedNodesFromU = pathOfU.getPath();
                    for (VisitedNode node : visitedNodesFromU) {
                        if (node.id().equals(lastStationId)) {
                            break;
                        }
                        visitedNodes.add(node);
                    }

                    VisitedNode lastNodeBeforeStation = visitedNodes.get(visitedNodes.size() - 1);

                    double lastStationTravelTime = lastNodeBeforeStation.travelTime() + this.getShortestEdgeBetweenNodes(lastNodeBeforeStation.id().getName(), lastStationId.getName()).duration + lastStationChargingTime;
                    double lastStationSocWithoutCharging = lastNodeBeforeStation.soc() - this.getShortestEdgeBetweenNodes(lastNodeBeforeStation.id().getName(), lastStationId.getName()).consumption;
                    double lastStationSocAfterCharging = this.calculateNewSoc(maxSoc, lastStationSocWithoutCharging, lastStationChargingTime, this.getNodeById(lastStationId.getName()).chargingPower);
                    VisitedNode visitedNodeLastStation = new VisitedNode(lastStationId.getName(), lastStationTravelTime, lastStationSocAfterCharging, lastStationChargingTime);
                    visitedNodes.add(visitedNodeLastStation);

                    VisitedNode visitedNodeV = new VisitedNode(v.id(), newTravelTimeV, lastStationSocAfterCharging - totalConsumption, 0.0);

                    for (int i = visitedNodes.size() - 1; i < visitedNodesFromU.size() - 1; i++) {
                        VisitedNode node = visitedNodes.get(i);
                        VisitedNode successor = visitedNodesFromU.get(i + 1);
                        double edgeConsumption = this.getShortestEdgeBetweenNodes(node.id().getName(), successor.id().getName()).consumption;
                        double edgeDuration = this.getShortestEdgeBetweenNodes(node.id().getName(), successor.id().getName()).duration;
                        VisitedNode visitedNode = new VisitedNode(successor.id().getName(), node.travelTime() + edgeDuration, node.soc() - edgeConsumption, successor.chargingTime());
                        visitedNodes.add(visitedNode);
                    }

                    visitedNodes.add(visitedNodeV);
                    Path path = new Path(visitedNodes);
                    pathOfNode.put(visitedNodeV.id(), path);
                    queue.put(visitedNodeV.id(), newTravelTimeV);
                }
                else {
                    // Ausreichend Energie, um zu v zu gelangen

                    List<VisitedNode> visitedNodes = new ArrayList<>(pathOfU.getPath());
                    VisitedNode visitedNodeV = new VisitedNode(v.id, currentTravelTime, currentSoc, 0.0);

                    queue.put(visitedNodeV.id(), currentTravelTime);

                    visitedNodes.add(visitedNodeV);

                    Path path = new Path(visitedNodes);
                    pathOfNode.put(visitedNodeV.id(), path);
                }
            }
        }

        if (result != null) {
            return result.getPath().stream()
                    .map(visitedNode -> new VisitedNodeResult(
                            visitedNode.id().getName(),
                            visitedNode.travelTime(),
                            visitedNode.soc(),
                            visitedNode.chargingTime()
                    ));
        } else {
            return Stream.empty();
        }
    }

    private void initialize(PriorityQueue queue, NodeContainer start, double initialCharge) {
        pathOfNode = new HashMap<>();


        List<VisitedNode> startPath = new ArrayList<>();
        VisitedNode startNode = new VisitedNode(start.id, 0.0, initialCharge, 0.0);
        queue.put(startNode.id(), 0.0);
        startPath.add(startNode);
        Path path = new Path(startPath);
        pathOfNode.put(startNode.id(), path);
    }

    private double calculateNewSoc(double maxSoc, double soc, double chargingTime, double chargingPower) {
        double chargedEnergy = (chargingTime / 60.0) * chargingPower;
        chargedEnergy = Math.round(chargedEnergy * 100.0) / 100.0;
        double newSoc = soc + chargedEnergy;
        return Math.min(newSoc, maxSoc);
    }

    private double calculateAdditionalChargeTime(double currentSoc, double necessarySoc, double chargingPower) {
        if (chargingPower == 0.0 || necessarySoc < currentSoc)
            return 0.0;
        double necessaryChargedEnergy = necessarySoc - currentSoc;
        double chargingTime = (necessaryChargedEnergy / chargingPower) * 60.0;
        chargingTime = Math.round(chargingTime * 100.0) / 100.0;
        return chargingTime;
    }

    private NodeContainer getNodeById(String nodeId) {
        Result result = tx.execute("MATCH (n) WHERE n.id = $id RETURN n AS node, n.id AS id, n.chargingPower AS chargingPower", Collections.singletonMap("id", nodeId));
        if (result.hasNext()) {
            Map<String, Object> row = result.next();
            Node node = (Node) row.get("node");
            String id = (String) node.getProperty("id", "");
            double chargingPower = ((Number) node.getProperty("chargingPower", 0.0)).doubleValue();
            return new NodeContainer(node, id, chargingPower);
        } else {
            return null;
        }
    }

    private List<EdgeContainer> getEdgesFromSourceNode(String nodeId) {
        NodeContainer sourceNodeContainer = getNodeById(nodeId);
        if (sourceNodeContainer != null) {
            List<EdgeContainer> edges = new ArrayList<>();
            for (Relationship relationship : sourceNodeContainer.node.getRelationships(Direction.OUTGOING)) {
                double duration = ((Number) relationship.getProperty("duration", 0.0)).doubleValue();
                double consumption = ((Number) relationship.getProperty("consumption", 0.0)).doubleValue();
                edges.add(new EdgeContainer(relationship, duration, consumption));
            }
            return edges;
        } else {
            return Collections.emptyList();
        }
    }

    private EdgeContainer getShortestEdgeBetweenNodes(String sourceId, String destinationId) {
        NodeContainer sourceNodeContainer = getNodeById(sourceId);
        NodeContainer destinationNodeContainer = getNodeById(destinationId);
        if (sourceNodeContainer != null && destinationNodeContainer != null) {
            Result result = tx.execute(
                    "MATCH (start)-[r]->(end) " +
                            "WHERE start.id = $sourceId AND end.id = $destinationId " +
                            "RETURN r AS relationship, r.duration AS duration, r.consumption AS consumption " +
                            "ORDER BY r.duration ASC LIMIT 1",
                    Map.of("sourceId", sourceId, "destinationId", destinationId));
            if (result.hasNext()) {
                Map<String, Object> row = result.next();
                Relationship relationship = (Relationship) row.get("relationship");
                double duration = ((Number) row.get("duration")).doubleValue();
                double consumption = ((Number) row.get("consumption")).doubleValue();
                return new EdgeContainer(relationship, duration, consumption);
            }
        }
        return null;
    }

}