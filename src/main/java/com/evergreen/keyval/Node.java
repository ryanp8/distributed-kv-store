package com.evergreen.keyval;

import com.fasterxml.jackson.core.type.TypeReference;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.Handler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;

public class Node extends ClusterMember {
    public Node(String hostname, int port, String[] nodes) {
        super(hostname, port, nodes);
        Javalin app = Javalin.create()
                .get("/db/{key}", this.handleClientGet())
                .post("/db/{key}", this.handleClientPost())
                .delete("/db/{key}", this.handleClientDelete())
                .get("/nodes", this.handleNodesGet())
                .post("/nodes", this.handleAllNodesPost())
                .post("/ring", this.handleRingPost())
                .get("/keys", this.handleKeysGet())
                .post("/keys", this.handleKeysPost())
                .delete("/keys", this.handleKeysDelete())
                .get("/{key}", this.handleDirectGet())
                .post("/{key}", this.handleDirectPost())
                .delete("/{key}", this.handleDirectDelete());
        app.start(port);
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

    private Handler handleClientGet() {
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

    private Handler handleNodeDelete() {
        return ctx -> {
            String address = ctx.body();
            long id = this.calculateID(address);
            this.deleteNode(id);
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
            HttpResponse<String> response = this.httpClient
                    .send(getNodesRequest, HttpResponse.BodyHandlers.ofString());
            String responseJson = response.body();
            synchronized (this) {
                this.updateMembership(responseJson, 0);
                this.nodeIdToAddress.put(this.id, this.address);
                this.nodes.add(this.id);
            }
            this.rebalanceOnAdd();
        };
    }

    private Handler handleKeysDelete() {
        return ctx -> {
            String upperBound = ctx.queryParam("upper");
            if (upperBound != null) {
                this.db.upperBoundDelete(Long.parseLong(upperBound));
            }
            ctx.status(200);
        };
    }

    private Handler handleKeysGet() {
        return ctx -> {
            String lowerBound = ctx.queryParam("lower");
            String upperBound = ctx.queryParam("upper");
            String jsonString;
            if (lowerBound != null && upperBound != null) {
                HashMap<String, String> result = this.db.boundGet(
                        Long.parseLong(lowerBound),
                        Long.parseLong(upperBound));
                jsonString = objectMapper.writeValueAsString(result);

            } else {
                HashMap<String, String> result = this.db.getAll();
                jsonString = objectMapper.writeValueAsString(result);
            }
            ctx.status(200);
            ctx.result(jsonString);
        };
    }

    private Handler handleKeysPost() {
        return ctx -> {
            try {
                String jsonString = ctx.body();
                HashMap<String, String> data = this.objectMapper.readValue(jsonString,
                        new TypeReference<>() {});
                for (String key : data.keySet()) {
                    this.db.post(key, data.get(key));
                }
                ctx.status(200);
            } catch (Exception e) {
                e.printStackTrace();
                ctx.status(500);
            }
        };
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
}
