package com.evergreen.keyval;

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
import java.util.*;

public class Coordinator extends Node {

    private MessageDigest md;
    private PriorityQueue<Integer> servants;
    private HashMap<Integer, String> servantIDtoAddress;
    public Coordinator(int port, String[] servants) {
        super(port);
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            System.err.println("No algorithm MD5");
        }
        this.servantIDtoAddress = new HashMap<>();
        List<Integer> servantIds = Arrays.stream(servants).map(this::calculateID).toList();
        for (int i = 0; i < servants.length; i++) {
            this.servantIDtoAddress.put(servantIds.get(i), servants[i]);
        }
        this.servants = new PriorityQueue<>(servantIds);
    }

    private int calculateID(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] digest = this.md.digest(keyBytes);
        return ByteBuffer.wrap(digest).getInt();
    }

    private ArrayList<String> calculateReplicaServers(String key) {
        Integer[] servants = this.servants.toArray(new Integer[0]);
        int hash = this.calculateID(key);
        int start = 0;
        int end = servants.length;
        while (start < end) {
            int mid = start + (end - start) / 2;
            if (servants[mid] > hash) {
                end = mid;
            } else if (servants[mid] < hash) {
                start = mid + 1;
            } else {
                start = mid;
                break;
            }
        }
        ArrayList<String> nodes = new ArrayList<>();
        int replicasAdded = 0;
        while (replicasAdded < Math.min(3, servants.length)) {
            nodes.add(this.servantIDtoAddress.get(servants[(start + replicasAdded) % servants.length]));
            replicasAdded++;
        }
        return nodes;
    }

    private void recursiveGet(Context ctx, String key, ArrayList<String> nodes, int step) {
        String node = nodes.get(step);
        String urlString = String.format("http://%s/%s", node, key);
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(urlString))
                    .GET()
                    .build();
            HttpResponse<String> response = HttpClient.newBuilder()
                    .build()
                    .send(request, HttpResponse.BodyHandlers.ofString());
            String value = response.body();
            if (value == null) {
                ctx.status(404);
                ctx.result("");
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

    protected Handler handleGet() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                ArrayList<String> nodes = this.calculateReplicaServers(key);
                this.recursiveGet(ctx, key, nodes, 0);
            };
        } catch (Exception e) {
            System.err.println("Unable to create get handler");
            return null;
        }
    }

    protected Handler handlePost() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                ArrayList<String> nodes = this.calculateReplicaServers(key);
                for (String node : nodes) {
                    String urlString = String.format("http://%s/%s", node, key);
                    String postValue = ctx.body();
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(new URI(urlString))
                                .POST(HttpRequest.BodyPublishers.ofString(postValue))
                                .build();
                        HttpResponse<String> response = HttpClient.newBuilder()
                                .build()
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
            return null;
        }
    }

    protected Handler handleDelete() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                ArrayList<String> nodes = this.calculateReplicaServers(key);
                for (String node : nodes) {
                    String urlString = String.format("http://%s/%s", node, key);
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(new URI(urlString))
                                .DELETE()
                                .build();
                        HttpResponse<String> response = HttpClient.newBuilder()
                                .build()
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
            System.err.println("Unable to create get handler");
            return null;
        }
    }
}