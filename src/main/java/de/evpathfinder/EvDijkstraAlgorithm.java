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
        if (start == null || end == null) {
            return Stream.empty();
        }

        PriorityQueue queue = new PriorityQueue();

        // Initialisierung des Algorithmus
        this.initialize(queue, start, initialCharge);

        Path result = null;

        while (!queue.isEmpty()) {
            // Aktuell bearbeiteter Knoten = u aus queue holen
            Path pathOfU = pathOfNode.get(queue.poll());
            VisitedNode u = pathOfU.getLastNode();

            // Falls u der Zielknoten ist, wird while-Schleife beendet
            if (u.id().getName().equals(end.id)) {
                // Endknoten gefunden. Dessen Nachbarn muessen nicht mehr ueberprueft werden.
                result = pathOfU;
                break;
            }

            // edgesFromU = Alle Kanten, die von u ausgehen
            List<EdgeContainer> edgesFromU = this.getEdgesFromSourceNode(u.id().getName());

            // Fuer alle Kanten, die von u ausgehen, folgende Schleife durchgehen
            for (EdgeContainer edgeFromU : edgesFromU) {
                // v = aktueller Nachbar dieses Schleifendurchlaufs
                NodeContainer v = this.getNodeById(edgeFromU.destinationId);


                // speichern der Eigenschaften der Kante in Variablen
                double duration = edgeFromU.duration;
                double consumption = edgeFromU.consumption;

                // Berechnungen der entsprechenden Reisezeit und des Ladestands bei v
                double currentTravelTime = pathOfU.getTravelTimeOfNode(u.id()) + duration;
                double currentSoc = pathOfU.getSocOfNode(u.id()) - consumption;

                // Ueberpruefung, ob der Ladestand bei v groesser oder kleiner als 0 ist
                if (currentSoc < 0) {
                    // Ladestand bei v kleiner als 0, nicht genuegend Energie vorhanden, um nach jetzigem Stand erreichen zu koennen

                    // Letzte Ladestation erhalten und Initialwerte fuer die darauffolgenden Ueberpruefungen setzen
                    VisitedNodeId lastStationId = pathOfU.getLastStation();
                    double lastStationChargingTime = 0.0;
                    double oldChargingTime = 0.0;
                    double totalConsumption = 0.0;
                    boolean lastStationChargedEnough = true;

                    // Ueberpruefung, ob bisher bei einer Ladestation geladen wurde, d.h. ob eine Ladestation gefunden wurde
                    if (lastStationId != null) {
                        // Falls bereits bei einer Ladestation geladen wurde
                        if (pathOfU.getSocOfNode(lastStationId) >= maxSoc) {
                            // Falls der Ladestand bei dieser Ladestation bereits auf 100 % ist, setze Flag, dass spaeter eine weitere Ladestation gefunden werden muss
                            lastStationChargedEnough = false;
                        }

                        // Gehe von jetzigem Knoten u bis zur Ladestation durch, um den Gesamtverbrauch von der Ladestation bis v zu berechnen und daraus die notwendige Ladezeit zu bestimmen
                        VisitedNodeId currentNodeId = u.id();
                        totalConsumption = consumption;
                        while (currentNodeId != null && !currentNodeId.equals(lastStationId)) {
                            VisitedNodeId parentNodeId = pathOfU.getParentOfNode(currentNodeId);
                            if (parentNodeId != null) {
                                totalConsumption += this.getShortestEdgeBetweenNodes(parentNodeId.getName(), currentNodeId.getName()).consumption;
                            }
                            currentNodeId = parentNodeId;
                        }

                        // Ueberprufung, ob der Gesamtverbrauch groesser als die maximale Akkukapazitaet ist
                        if (totalConsumption > maxSoc) {
                            // Falls der Gesamtverbrauch groeßer als die maximale Akkukapazitaet ist, reicht das Laden bei dieser Ladestation nicht aus, um v erreichen zu koennen
                            lastStationChargedEnough = false;
                            oldChargingTime = 0.0;
                        } else {
                            // Laden bei dieser Ladestation reicht aus, um v erreichen zu koennen, weshalb die neue Ladezeit fuer diese Ladestation berechnet und in einer Variable gespeichert wird
                            double socWithOutCharging = pathOfU.getSocOfNode(pathOfU.getParentOfNode(lastStationId)) - this.getShortestEdgeBetweenNodes(pathOfU.getParentOfNode(lastStationId).getName(), lastStationId.getName()).consumption;
                            lastStationChargingTime = this.calculateAdditionalChargeTime(socWithOutCharging, totalConsumption, this.getNodeById(lastStationId.getName()).chargingPower);
                            // speichere die bisherige Ladezeit der Ladestation in einer Variable
                            oldChargingTime = pathOfU.getChargingTimeOfNode(lastStationId);
                        }
                    }

                    boolean newLastStationAdded = false;
                    // Ueberpruefung, ob eine Ladestation gefunden wurde, oder ob bei einer bereits geladenen Ladestation ausreichend nachgeladen wurde
                    if (lastStationId == null || !lastStationChargedEnough) {
                        // Bisher keine Ladestation gefunden oder es konnte bei einer bereits geladenen Ladestation nicht ausreichend zusaetzlich geladen werden

                        // Suche nach allen Ladestationen von u bis p und speichere diese in lastStations mit zu ladender Energiemenge
                        // Falls der Verbrauch groeßer als die maximale Akkukapazitaet wird, kann die Suche abgebrochen werden
                        totalConsumption = consumption;
                        VisitedNodeId currentNodeId = u.id();
                        Map<VisitedNodeId, Double> lastStations = new HashMap<>();
                        while (currentNodeId != null && !currentNodeId.equals(lastStationId)) {
                            if (this.getNodeById(currentNodeId.getName()).chargingPower > 0 && totalConsumption < maxSoc) {
                                // Falls dieser Knoten eine Ladestation ist und noch ausreichend Energie dort geladen werden kann, fuege diese Ladestation zur Auswahl hinzu
                                lastStations.put(currentNodeId, totalConsumption);
                            }
                            // Erhalte Parent von diesem Knoten fuer weitere Suche und falls Parent vorhanden, addiere den Kantenverbrauch zum Gesamtverbrauch hinzu
                            VisitedNodeId parentNodeId = pathOfU.getParentOfNode(currentNodeId);
                            if (parentNodeId != null) {
                                totalConsumption += this.getShortestEdgeBetweenNodes(parentNodeId.getName(), currentNodeId.getName()).consumption;
                            }
                            currentNodeId = parentNodeId;
                        }

                        lastStationId = null;
                        // Gehe gefundene Ladestationen durch
                        double newChargingTimeLastStation = Double.MAX_VALUE;
                        for (Map.Entry<VisitedNodeId, Double> lastStation : lastStations.entrySet()) {
                            VisitedNodeId w = lastStation.getKey();
                            double necessarySoc = lastStation.getValue();
                            if (necessarySoc > maxSoc) {
                                // Falls Gesamtverbrauch (notwendige Energiemenge) groesser als die maximale Akkukapazitaet ist, kann bei dieser und allen folgenden Ladestation nicht geladen werden
                                break;
                            }
                            if (pathOfU.getSocOfNode(w) > necessarySoc) {
                                // Falls bei dieser Ladestation der Ladestand bereits groesser als die notwendige Energiemenge ist, wird direkt diese Ladestation gewaehlt
                                lastStationId = w;
                                newLastStationAdded = true;
                                break;
                            }
                            // Berechne die neue Ladezeit bei dieser Ladestation
                            double tempNewChargingTimeLastStation = this.calculateAdditionalChargeTime(pathOfU.getSocOfNode(w), necessarySoc, this.getNodeById(w.getName()).chargingPower);
                            if (tempNewChargingTimeLastStation < newChargingTimeLastStation) {
                                // Falls die Ladezeit der aktuellen Ladestation kleiner ist, als die bisher beste Ladezeit, wird diese Ladestation als neue beste Ladestation gewaehlt
                                newChargingTimeLastStation = tempNewChargingTimeLastStation;
                                lastStationId = w;
                                lastStationChargingTime = newChargingTimeLastStation;
                                totalConsumption = necessarySoc;
                                newLastStationAdded = true;
                            }
                        }
                    }

                    if (lastStationId == null) {
                        // Es konnte keine Ladestation gefunden werden, weshalb v ueber diesen Weg nicht erreichbar ist
                        continue;
                    }

                    if (minChargingTime > lastStationChargingTime && pathOfU.getNodeById(lastStationId).soc() < totalConsumption) {
                        // Falls die berechnete Ladestation kleiner als die gewuenschte Minimalladezeit ist und tatsaechlich geladen werden muss, dann setze die Ladezeit auf die gewuenschte Ladezeit
                        lastStationChargingTime = minChargingTime;
                    }

                    // Erstelle den neuen Weg nach v
                    List<VisitedNode> visitedNodes = new ArrayList<>();

                    int lastStationIndex = -1;
                    VisitedNodeId oldLastStationId = null;
                    // Speichere alle Knoten nach u in einer separaten Liste
                    List<VisitedNode> visitedNodesFromU = pathOfU.getPath();
                    // ID von der letzten Ladestation erhalten
                    for (int i = visitedNodesFromU.size() - 1; i >= 0; i--) {
                        if (visitedNodesFromU.get(i).chargingTime() > 0.0) {
                            lastStationIndex = i;
                            oldLastStationId = visitedNodesFromU.get(i).id();
                            break;
                        }
                    }
                    // Ueberpruefe, ob bisher eine Ladestation existiert hat und ob eine neue Ladestation hinzugefuegt wurde
                    if (lastStationIndex != -1 && newLastStationAdded) {
                        // Falls bisher eine Ladestation existiert hat und eine neue Ladestation hinzugefuegt wurde
                        // fuege alle Knoten von Start bis zu der zuletzt geladenen Ladestation zum Weg hinzu
                        for (int i = 0; i < lastStationIndex; i++) {
                            visitedNodes.add(visitedNodesFromU.get(i));
                        }
                        // Berechne Verbrauch von bisher letzter Ladestation zu neuer Ladestation
                        double consumptionFromOldStationToNewStation = 0.0;
                        for (int i = lastStationIndex; i < visitedNodesFromU.size() - 1; i++) {
                            consumptionFromOldStationToNewStation += this.getShortestEdgeBetweenNodes(visitedNodesFromU.get(i).id().getName(), visitedNodesFromU.get(i+1).id().getName()).consumption;
                            if (visitedNodesFromU.get(i + 1).id().equals(lastStationId)) {
                                break;
                            }
                        }
                        // Erstelle Werte fuer die Ladestation bei der bisher zuletzt geladen wurde und erstelle einen neuen VisitedNode fuer diese und fuege sie zum Weg hinzu
                        VisitedNode lastNodeBeforeLastStation = visitedNodesFromU.get(lastStationIndex - 1);
                        double oldLastStationChargingPower = this.getNodeById(oldLastStationId.getName()).chargingPower;
                        double oldLastStationSocWithoutCharging = lastNodeBeforeLastStation.soc() - this.getShortestEdgeBetweenNodes(lastNodeBeforeLastStation.id().getName(), visitedNodesFromU.get(lastStationIndex).id().getName()).consumption;
                        double oldLastStationChargingTime = this.calculateAdditionalChargeTime(oldLastStationSocWithoutCharging, consumptionFromOldStationToNewStation, oldLastStationChargingPower);
                        if (oldLastStationChargingTime < minChargingTime) {
                            oldLastStationChargingTime = minChargingTime;
                        }
                        double oldLastStationTravelTime = lastNodeBeforeLastStation.travelTime() + this.getShortestEdgeBetweenNodes(lastNodeBeforeLastStation.id().getName(), visitedNodesFromU.get(lastStationIndex).id().getName()).duration + oldLastStationChargingTime;
                        double oldLastStationSocAfterCharging = this.calculateNewSoc(maxSoc, oldLastStationSocWithoutCharging, oldLastStationChargingTime, oldLastStationChargingPower);
                        VisitedNode oldLastStation = new VisitedNode(oldLastStationId.getName(), oldLastStationTravelTime, oldLastStationSocAfterCharging, oldLastStationChargingTime);
                        visitedNodes.add(oldLastStation);
                        // Fuege alle Knoten von der zuletzt geladenen Ladestation zur neu hinzgefuegten Ladestation zum Weg hinzu
                        for (int i = lastStationIndex; i < visitedNodesFromU.size() - 1; i++) {
                            VisitedNode node = visitedNodes.get(i);
                            VisitedNode successor = visitedNodesFromU.get(i + 1);
                            if (successor.id().equals(lastStationId)) {
                                break;
                            }
                            double edgeConsumption = this.getShortestEdgeBetweenNodes(node.id().getName(), successor.id().getName()).consumption;
                            double edgeDuration = this.getShortestEdgeBetweenNodes(node.id().getName(), successor.id().getName()).duration;
                            VisitedNode visitedNode = new VisitedNode(successor.id().getName(), node.travelTime() + edgeDuration, node.soc() - edgeConsumption, successor.chargingTime());
                            visitedNodes.add(visitedNode);
                        }
                    } else {
                        // Falls bisher keine Ladestation existiert hat oder keine neue Ladestation hinzugefuegt wurde
                        // Uebernehme alle Knoten start bis zur neuen Ladestation von dem Weg nach u
                        for (VisitedNode node : visitedNodesFromU) {
                            if (node.id().equals(lastStationId)) {
                                break;
                            }
                            visitedNodes.add(node);
                        }
                    }

                    VisitedNode lastNodeBeforeStation = visitedNodes.get(visitedNodes.size() - 1);

                    // Erstelle ein neues VisitedNode-Objekt fuer die neue Ladestation
                    double lastStationTravelTime = lastNodeBeforeStation.travelTime() + this.getShortestEdgeBetweenNodes(lastNodeBeforeStation.id().getName(), lastStationId.getName()).duration + lastStationChargingTime;
                    double lastStationSocWithoutCharging = lastNodeBeforeStation.soc() - this.getShortestEdgeBetweenNodes(lastNodeBeforeStation.id().getName(), lastStationId.getName()).consumption;
                    double lastStationSocAfterCharging = this.calculateNewSoc(maxSoc, lastStationSocWithoutCharging, lastStationChargingTime, this.getNodeById(lastStationId.getName()).chargingPower);
                    VisitedNode visitedNodeLastStation = new VisitedNode(lastStationId.getName(), lastStationTravelTime, lastStationSocAfterCharging, lastStationChargingTime);
                    visitedNodes.add(visitedNodeLastStation);

                    double newSocV = lastStationSocAfterCharging - totalConsumption;

                    // Fuege alle Knoten mit den angepassten Werten von der Ladestation an bis einschließlich u zur Liste hinzu
                    for (int i = visitedNodes.size() - 1; i < visitedNodesFromU.size() - 1; i++) {
                        VisitedNode node = visitedNodes.get(i);
                        VisitedNode successor = visitedNodesFromU.get(i + 1);
                        double  edgeConsumption = this.getShortestEdgeBetweenNodes(node.id().getName(), successor.id().getName()).consumption;
                        double edgeDuration = this.getShortestEdgeBetweenNodes(node.id().getName(), successor.id().getName()).duration;
                        VisitedNode visitedNode = new VisitedNode(successor.id().getName(), node.travelTime() + edgeDuration, node.soc() - edgeConsumption, successor.chargingTime());
                        visitedNodes.add(visitedNode);
                    }

                    // Erstelle ein neues VisitedNode-Objekt fuer v
                    VisitedNode newU = visitedNodes.get(visitedNodes.size() - 1);
                    // Berechne die neue Reisezeit von Start nach v
                    double newTravelTimeV = newU.travelTime() + this.getShortestEdgeBetweenNodes(newU.id().getName(), v.id()).duration;
                    VisitedNode visitedNodeV = new VisitedNode(v.id(), newTravelTimeV, newSocV, 0.0);
                    // Ueberpruefe, ob der neue Zustand von v schlechter als irgendein anderer Zustand in V ist
                    if (this.checkIfCurrentNodeIsBetter(visitedNodeV)) {
                        // Vervollstaendige die Liste durch Hinzufuegen von v und erstelle ein Path-Objekt und fuege dieses zu pathOfNode hinzu, sowie den VisitedNode von v zur Queue
                        visitedNodes.add(visitedNodeV);
                        Path path = new Path(visitedNodes);
                        pathOfNode.put(visitedNodeV.id(), path);
                        queue.put(visitedNodeV.id(), newTravelTimeV);
                    }
                }
                else {
                    // Ladestand bei v groesser als 0, genuegend Energie vorhanden, um v erreichen zu koennen, sodass nicht geladen werden muss

                    // Uebernehme den Pfad nach u
                    List<VisitedNode> visitedNodes = new ArrayList<>(pathOfU.getPath());
                    // Erstelle ein VisitedNode-Objekt fuer v, fuege dieses sowohl zur Liste fuer den neuen Pfad als auch zur Queue hinzu
                    VisitedNode visitedNodeV = new VisitedNode(v.id(), currentTravelTime, currentSoc, 0.0);
                    // Ueberpruefe, ob der neue Zustand von v schlechter als irgendein anderer Zustand in V ist
                    if (this.checkIfCurrentNodeIsBetter(visitedNodeV)) {
                        visitedNodes.add(visitedNodeV);
                        queue.put(visitedNodeV.id(), currentTravelTime);
                        // Erstelle einen neuen Pfad fuer den Knoten v
                        Path path = new Path(visitedNodes);
                        pathOfNode.put(visitedNodeV.id(), path);
                    }
                }
            }
        }

        // Ueberpruefe, ob ein Pfad gefunden wurde
        if (result != null) {
            // Falls ein Pfad zum Zielknoten gefunden wurde, gebe diesen als Stream von VisitedNodeResults zurueck
            return result.getPath().stream()
                    .map(visitedNode -> new VisitedNodeResult(
                            visitedNode.id().getName(),
                            visitedNode.travelTime(),
                            visitedNode.soc(),
                            visitedNode.chargingTime()
                    ));
        } else {
            // Falls kein Pfad gefunden werden konnte, gebe einen leeren Stream zurueck
            return Stream.empty();
        }
    }

    private void initialize(PriorityQueue queue, NodeContainer start, double initialCharge) {
        pathOfNode = new HashMap<>();

        // Erstelle eine Liste fuer den Weg des Startknotens mit diesem als VisitedNode darin enthalten
        List<VisitedNode> startPath = new ArrayList<>();
        VisitedNode startNode = new VisitedNode(start.id, 0.0, initialCharge, 0.0);
        startPath.add(startNode);
        Path path = new Path(startPath);
        pathOfNode.put(startNode.id(), path);
        // Fuege ausschließlich den Startknoten zur Priority Queue hinzu
        queue.put(startNode.id(), 0.0);
    }

    private boolean checkIfCurrentNodeIsBetter(VisitedNode visitedNode) {
        for (Map.Entry<VisitedNodeId, Path> entry : pathOfNode.entrySet()) {
            VisitedNodeId nodeId = entry.getKey();
            Path path = entry.getValue();

            // Prüfen, ob der Name des aktuellen Knotens mit dem Namen des Knotens im Path übereinstimmt
            if (nodeId.getName().equals(visitedNode.id().getName())) {
                VisitedNode nodeInPath = path.getNodeById(nodeId);

                // Wenn der Knoten gefunden wurde, vergleichen Sie die Reisezeit und den SOC
                if (nodeInPath != null) {
                    if (visitedNode.travelTime() > nodeInPath.travelTime() && visitedNode.soc() < nodeInPath.soc()) {
                        // Der aktuelle Knoten ist schlechter oder gleich in Bezug auf Reisezeit und SOC
                        return false;
                    } else {
                        // Der aktuelle Knoten ist besser in Bezug auf Reisezeit und SOC
                        continue;
                    }
                }
            }
        }
        // Der aktuelle Knoten ist besser als alle Knoten mit demselben Namen im Path
        return true;
    }

    private double calculateNewSoc(double maxSoc, double soc, double chargingTime, double chargingPower) {
        // Berechne aus dem aktuellen SoC und der chargingTime den neuen Ladestand in kWh abhaengig von Ladeleistung und maximaler Akkukapazitaet
        double chargedEnergy = (chargingTime / 60.0) * chargingPower;
        chargedEnergy = Math.round(chargedEnergy * 100.0) / 100.0;
        double newSoc = soc + chargedEnergy;
        return Math.min(newSoc, maxSoc);
    }

    private double calculateAdditionalChargeTime(double currentSoc, double necessarySoc, double chargingPower) {
        // Berechne die benoetigte Ladezeit, um von dem aktuellen SoC auf den neuen SoC zu gelangen in min abhaengig von der Ladeleistung
        if (chargingPower == 0.0 || necessarySoc < currentSoc)
            return 0.0;
        double necessaryChargedEnergy = necessarySoc - currentSoc;
        double chargingTime = (necessaryChargedEnergy / chargingPower) * 60.0;
        chargingTime = Math.round(chargingTime * 100.0) / 100.0;
        return chargingTime;
    }

    private NodeContainer getNodeById(String nodeId) {
        // Liefert den Node aus der Datenbank anhand seiner ID zurueck
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
        // Liefert alle Edges, die von einem bestimmten Knoten aus gehen zurueck
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
        // Liefert die kuerzeste Kante zwischen zwei Knoten zurueck
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