package com.evergreen.keyval;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.Handler;

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

public class Node {

    private int replicas;
    private MessageDigest md;
    private PriorityQueue<Long> nodes;
    private HashMap<Long, String> nodeIdToAddress;
    private final DBClient db;
    private final long id;
    private final String address;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private long nodesUpdatedTime;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public Node(String hostname, int port, String[] nodes) {
        Javalin app = Javalin.create()
                .get("/db/{key}", this.handleClientGet())
                .post("/db/{key}", this.handleClientPost())
                .delete("/db/{key}", this.handleClientDelete())
                .get("/nodes", this.handleNodesGet())
                .post("/nodes", this.handleAllNodesPost())
                .post("/ring", this.handleRingPost())
                .get("/{key}", this.handleDirectGet())
                .post("/{key}", this.handleDirectPost())
                .delete("/{key}", this.handleDirectDelete());
        app.start(port);


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

        this.replicas = Math.min(3, nodeIds.size());

        TimerTask pollNodes = new TimerTask() {
            @Override
            public void run() {
                int targetNodeIdx = (int) (Math.random() * Node.this.nodes.size());
                long targetNode = Node.this.nodes.toArray(new Long[0])[targetNodeIdx];
                String urlString = String.format("http://%s/nodes", Node.this.nodeIdToAddress.get(targetNode));
                try {
                    // Exchange node information between two nodes. POST to the partner node and then GET from it
                    String jsonString = objectMapper.writeValueAsString(Node.this.nodeIdToAddress);
                    HttpRequest postNodesRequest = HttpRequest.newBuilder()
                            .uri(new URI(urlString))
                            .header("Last-Modified", String.valueOf(Node.this.nodesUpdatedTime))
                            .POST(HttpRequest.BodyPublishers.ofString(jsonString))
                            .build();
                    Node.this.httpClient.send(postNodesRequest, HttpResponse.BodyHandlers.ofString());

                    HttpRequest getNodesRequest = HttpRequest.newBuilder()
                            .uri(new URI(urlString))
                            .GET()
                            .build();
                    HttpResponse<String> response = Node.this.httpClient
                            .send(getNodesRequest, HttpResponse.BodyHandlers.ofString());
                    String responseJson = response.body();
                    Optional<String> lastModifiedHeader = response.headers().firstValue("Last-Modified");

                    // Keep the node information that was updated more recently
                    if (lastModifiedHeader.isPresent()) {
                        long lastModified = Long.parseLong(lastModifiedHeader.get());
                        Node.this.updateMembership(responseJson, lastModified);
                    }
                    Node.this.replicas = Math.min(3, Node.this.nodes.size());

                } catch (InterruptedException | IOException e) {
                    Node.this.deleteNode(targetNode);
                    System.err.printf("Node at %s was removed from the ring\n", Node.this.nodeIdToAddress.get(targetNode));
                } catch (URISyntaxException e) {
                    System.err.printf("Unable to create URI %s\n", urlString);
                }
            }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(pollNodes, 3000,1000);
    }

    private long calculateID(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] digest = this.md.digest(keyBytes);
        return ByteBuffer.wrap(digest).getLong();
    }

    private List<Long> calculatePreferenceList(String key) {
        Long[] nodes = this.nodes.toArray(new Long[0]);
        long hash = this.calculateID(key);
        int start = 0;
        int end = nodes.length;
        if (hash > nodes[end - 1]) {
            while (start < end) {
                int mid = start + (end - start) / 2;
                if (nodes[mid] > hash) {
                    end = mid;
                } else if (nodes[mid] < hash) {
                    start = mid + 1;
                } else {
                    start = mid;
                    break;
                }
            }
        }

        ArrayList<Long> preferenceList = new ArrayList<>();
        int nodesAdded = 0;
        while (nodesAdded < this.replicas) {
            preferenceList.add(nodes[(start + nodesAdded) % nodes.length]);
            nodesAdded++;
        }
        return preferenceList;
    }

    private synchronized void updateMembership(String membershipJson, long lastModified) {
        if (lastModified > Node.this.nodesUpdatedTime || lastModified == 0) {
            try {
                this.nodeIdToAddress = objectMapper.readValue(membershipJson,
                        new TypeReference<>() {});
                this.nodes = new PriorityQueue<>(nodeIdToAddress.keySet());
                this.nodesUpdatedTime = Instant.now().toEpochMilli();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private synchronized void deleteNode(long nodeId) {
        this.nodeIdToAddress.remove(nodeId);
        this.nodes.remove(nodeId);
    }

    private void recursiveGet(Context ctx, String key, List<String> nodes, int step) {
        String node = nodes.get(step);
        String urlString = String.format("http://%s/%s", node, key);
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(urlString))
                    .GET()
                    .build();
            HttpResponse<String> response = this.httpClient
                    .send(request, HttpResponse.BodyHandlers.ofString());
            String value = response.body();
            if (value == null) {
                ctx.status(404);
            } else {
                ctx.status(200);
                ctx.result(value);
            }
        } catch (InterruptedException | IOException e) {
            if (step >= nodes.size()) {
                ctx.status(404);
            } else {
                this.recursiveGet(ctx, key, nodes, step + 1);
            }
        } catch (URISyntaxException e) {
            System.err.printf("Unable to create URI %s\n", urlString);
        }
    }

    protected Handler handleClientGet() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                final byte[] val = this.db.get(key);
                if (val == null) {
                    List<Long> preferenceList = this.calculatePreferenceList(key);
                    List<String> preferenceAddresses = preferenceList.stream().map(nodeIdToAddress::get).toList();
                    this.recursiveGet(ctx, key, preferenceAddresses.subList(1, preferenceAddresses.size()), 0);
                } else {
                    ctx.result(new String(val));
                    ctx.status(200);
                }
            };
        } catch (Exception e) {
            System.err.println("Unable to create get handler");
            return ctx -> {
                ctx.status(500);
            };
        }
    }

    private Handler handleClientPost() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                List<Long> preferenceList = this.calculatePreferenceList(key);
                for (Long nodeId : preferenceList) {
                    String postValue = ctx.body();
                    String nodeAddress = this.nodeIdToAddress.get(nodeId);
                    String urlString = String.format("http://%s/%s", nodeAddress, key);
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(new URI(urlString))
                                .POST(HttpRequest.BodyPublishers.ofString(postValue))
                                .build();
                        HttpResponse<String> response = this.httpClient
                                .send(request, HttpResponse.BodyHandlers.ofString());
                        String responseValue = response.body();
                        if (responseValue == null) {
                            ctx.status(404);
                        } else {
                            ctx.status(200);
                            ctx.result(responseValue);
                        }
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                    } catch (URISyntaxException e) {
                        System.err.printf("Unable to create URI %s\n", urlString);
                    }
                }
            };
        } catch (Exception e) {
            System.err.println("Unable to create get handler");
            return ctx -> {
                ctx.status(500);
            };
        }
    }

    private Handler handleClientDelete() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                List<Long> preferenceList = this.calculatePreferenceList(key);
                for (Long nodeId : preferenceList) {
                    String nodeAddress = this.nodeIdToAddress.get(nodeId);
                    String urlString = String.format("http://%s/%s", nodeAddress, key);
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(new URI(urlString))
                                .DELETE()
                                .build();
                        HttpResponse<String> response = this.httpClient
                                .send(request, HttpResponse.BodyHandlers.ofString());
                        ctx.status(response.statusCode());
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                    } catch (URISyntaxException e) {
                        System.err.printf("Unable to create URI %s\n", urlString);
                    }
                }
            };
        } catch (Exception e) {
            System.err.println("Unable to create delete handler");
            return ctx -> {
                ctx.status(500);
            };
        }
    }

    private Handler handleDirectGet() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                final byte[] result = this.db.get(key);
                if (result == null) {
                    ctx.status(404);
                } else {
                    ctx.status(200);
                    ctx.result(new String(result));
                }
            };
        } catch (Exception e) {
            System.err.println("Unable to create delete handler");
            return ctx -> {
                ctx.status(500);
            };
        }
    }

    private Handler handleDirectPost() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                String value = ctx.body();
                final byte[] result = this.db.post(key, value);
                if (result == null) {
                    ctx.status(500);
                } else {
                    ctx.status(200);
                    ctx.result(new String(result));
                }
            };
        } catch (Exception e) {
            System.err.println("Unable to create delete handler");
            return ctx -> {
                ctx.status(500);
            };
        }
    }

    private Handler handleDirectDelete() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                final byte[] result = this.db.delete(key);
                if (result == null) {
                    ctx.status(404);
                } else {
                    ctx.status(200);
                    ctx.result(new String(result));
                }
            };
        } catch (Exception e) {
            System.err.println("Unable to create delete handler");
            return ctx -> {
                ctx.status(500);
            };
        }
    }

    // curl http://localhost:3000/ring -X POST -d localhost:3001
    // joins the ring of the node provided in request body
    private Handler handleRingPost() {
        return ctx -> {
            // Get the other members of the ring from the current member we are using to join the ring
            // Set the node's members to the members of the current node
            String address = ctx.body();
            HttpRequest getNodesRequest = HttpRequest.newBuilder()
                    .uri(new URI(String.format("http://%s/nodes", address)))
                    .GET()
                    .build();
            HttpResponse<String> response = Node.this.httpClient
                    .send(getNodesRequest, HttpResponse.BodyHandlers.ofString());
            String responseJson = response.body();
            Node.this.updateMembership(responseJson, 0);

            // Add the newly added node the membership data
            // TODO: Should reconcile membership versions instead of just adding the new node
            this.nodes.add(this.id);
            this.nodeIdToAddress.put(this.id, this.address);
        };
    }

    private Handler handleNodeDelete() {
        return ctx -> {
            String address = ctx.body();
            long id = this.calculateID(address);
            this.deleteNode(id);
        };
    }

    private Handler handleAllNodesPost() {
        return ctx -> {
            String lastModifiedHeader = ctx.header("Last-Modified");
            String jsonString = ctx.body();
            if (lastModifiedHeader != null) {
                long lastModified = Long.parseLong(lastModifiedHeader);
                this.updateMembership(jsonString, lastModified);
            }
            ctx.status(200);
        };
    }

    private Handler handleNodesGet() {
        try {
            return ctx -> {
                String jsonString = objectMapper.writeValueAsString(this.nodeIdToAddress);
                ctx.header("Last-Modified", String.valueOf(this.nodesUpdatedTime));
                ctx.status(200);
                ctx.result(jsonString);
            };
        } catch (Exception e) {
            e.printStackTrace();
            return ctx -> {
                ctx.status(500);
            };
        }
    }
}
