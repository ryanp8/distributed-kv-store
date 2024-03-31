package com.evergreen.keyval;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClusterMember {

    protected int replicas;
    protected MessageDigest md;
    protected PriorityQueue<Long> nodes;
    protected Long[] nodesArray;
    protected HashMap<Long, String> nodeIdToAddress;
    protected final DBClient db;
    protected final long id;
    protected final String address;
    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected long nodesUpdatedTime;
    protected final HttpClient httpClient = HttpClient.newHttpClient();

    public ClusterMember(String hostname, int port, String[] nodes) {

        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            System.err.println("No algorithm MD5");
        }
        this.db = new DBClient(port);
        this.address = String.format("%s:%d", hostname, port);
        this.id = this.calculateID(this.address);
        this.nodeIdToAddress = new HashMap<>();
        ArrayList<Long> nodeIds = new ArrayList<>(Arrays.stream(nodes).map(this::calculateID).toList());
        nodeIds.add(this.id);
        for (int i = 0; i < nodes.length; i++) {
            this.nodeIdToAddress.put(nodeIds.get(i), nodes[i]);
        }
        nodeIdToAddress.put(this.id, String.format("%s:%d", hostname, port));
        this.nodes = new PriorityQueue<>(nodeIds);
        this.nodesUpdatedTime = Instant.now().toEpochMilli();

        this.replicas = Math.min(2, nodeIds.size());

        TimerTask pollNodes = new TimerTask() {
            @Override
            public void run() {
                ClusterMember outerThis = ClusterMember.this;
                int targetNodeIdx = (int) (Math.random() * outerThis.nodes.size());
                long targetNode = outerThis.nodes.toArray(new Long[0])[targetNodeIdx];
                String urlString = String.format("http://%s/nodes", outerThis.nodeIdToAddress.get(targetNode));
                try {
                    // Exchange node information between two nodes. POST to the partner node and then GET from it
                    String jsonString = objectMapper.writeValueAsString(outerThis.nodeIdToAddress);
                    HttpRequest postNodesRequest = HttpRequest.newBuilder()
                            .uri(new URI(urlString))
                            .header("Last-Modified", String.valueOf(outerThis.nodesUpdatedTime))
                            .POST(HttpRequest.BodyPublishers.ofString(jsonString))
                            .build();
                    outerThis.httpClient.send(postNodesRequest, HttpResponse.BodyHandlers.ofString());

                    HttpRequest getNodesRequest = HttpRequest.newBuilder()
                            .uri(new URI(urlString))
                            .GET()
                            .build();
                    HttpResponse<String> response = outerThis.httpClient
                            .send(getNodesRequest, HttpResponse.BodyHandlers.ofString());
                    String responseJson = response.body();
                    Optional<String> lastModifiedHeader = response.headers().firstValue("Last-Modified");

                    // Keep the node information that was updated more recently
                    if (lastModifiedHeader.isPresent()) {
                        long lastModified = Long.parseLong(lastModifiedHeader.get());
                        outerThis.updateMembership(responseJson, lastModified);
                    }
                    outerThis.replicas = Math.min(2, outerThis.nodes.size());

                } catch (InterruptedException | IOException e) {
                    System.err.printf("Node at %s was removed from the ring\n", outerThis.nodeIdToAddress.get(targetNode));
                    outerThis.deleteNode(targetNode);
                } catch (URISyntaxException e) {
                    System.err.printf("Unable to create URI %s\n", urlString);
                }
            }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(pollNodes, 3000,1000);
    }

    protected long calculateID(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] digest = this.md.digest(keyBytes);
        return ByteBuffer.wrap(digest).getLong();
    }

    protected int calculateNodeIdx(String address) {
        Long[] nodes = this.nodes.toArray(new Long[0]);
        long hash = this.calculateID(address);
        return this.calculateNodeIdx(hash);
    }

    protected int calculateNodeIdx(long nodeId) {
        Long[] nodes = this.nodes.toArray(new Long[0]);
        int start = 0;
        int end = nodes.length;
        while (start < end) {
            int mid = start + (end - start) / 2;
            if (nodes[mid] > nodeId) {
                end = mid;
            } else if (nodes[mid] < nodeId) {
                start = mid + 1;
            } else {
                return mid;
            }
        }
        return start;
    }

    protected List<Long> calculatePreferenceList(String key) {
        Long[] nodes = this.nodes.toArray(new Long[0]);
        long nodeIdx = this.calculateNodeIdx(key);

        ArrayList<Long> preferenceList = new ArrayList<>();
        int nodesAdded = 0;
        while (nodesAdded < this.replicas) {
            preferenceList.add(nodes[((int) nodeIdx + nodesAdded) % nodes.length]);
            nodesAdded++;
        }
        return preferenceList;
    }

    protected void rebalanceOnAdd() {
        Long[] nodes = this.nodes.toArray(new Long[0]);
        int nodeIdx = this.calculateNodeIdx(this.id);

        int prevNodeIdx = nodeIdx - 1;
        if (prevNodeIdx < 0) {
            prevNodeIdx = nodes.length - 1;
        }
        long prevNodeId = nodes[prevNodeIdx];
        String prevNodeAddress = this.nodeIdToAddress.get(prevNodeId);

        int lowerNodeIdx = nodeIdx - this.replicas;
        if (lowerNodeIdx < 0) {
            lowerNodeIdx = nodes.length - this.replicas + nodeIdx;
        }
        long lowerBound = nodes[lowerNodeIdx];

        try {
            HttpRequest getKeysRequest = HttpRequest.newBuilder()
                    .uri(new URI(String.format("http://%s/keys?lower=%d&upper=%d", prevNodeAddress, lowerBound, this.id)))
                    .GET()
                    .build();
            HttpResponse<String> response = this.httpClient.send(getKeysRequest, HttpResponse.BodyHandlers.ofString());
            HashMap<String, String> data = this.objectMapper.readValue(response.body(), new TypeReference<>() {});
            for (String key : data.keySet()) {
                this.db.post(key, data.get(key));
            }

            for (int i = 0; i < this.replicas; i++) {
                long deleteUpperBound = nodes[(lowerNodeIdx + i + 1) % nodes.length];
                String nodeAddress = this.nodeIdToAddress.get(nodes[(nodeIdx + i + 1) % nodes.length]);
                HttpRequest deleteKeysRequest = HttpRequest.newBuilder()
                        .uri(new URI(String.format("http://%s/keys?upper=%d", nodeAddress, deleteUpperBound)))
                        .DELETE()
                        .build();
                this.httpClient.send(deleteKeysRequest, HttpResponse.BodyHandlers.ofString());
            }
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    protected synchronized void updateMembership(String membershipJson, long lastModified) {
        if (lastModified > this.nodesUpdatedTime || lastModified == 0) {
            try {
                HashMap<Long, String> newNodes = objectMapper.readValue(membershipJson,
                        new TypeReference<>() {});
                if (!newNodes.equals(this.nodeIdToAddress)) {
                    Set<Long> combinedKeys = Stream.concat(newNodes.keySet().stream(),
                                    this.nodeIdToAddress.keySet().stream())
                            .collect(Collectors.toSet());
                    this.nodes = new PriorityQueue<>(combinedKeys);
                    for (Long key : newNodes.keySet()) {
                        if (newNodes.get(key).isEmpty()) {
                            this.nodes.remove(key);
                        }
                        this.nodeIdToAddress.put(key, newNodes.get(key));
                    }
                    this.nodesUpdatedTime = Instant.now().toEpochMilli();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected synchronized void deleteNode(long nodeId) {
        this.nodeIdToAddress.remove(nodeId);
        this.nodes.remove(nodeId);
        this.nodesUpdatedTime = Instant.now().toEpochMilli();
    }

}
