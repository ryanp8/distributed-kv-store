package com.evergreen.keyval;


import io.javalin.http.Handler;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Options;

public class Servant extends Node {

    private RocksDB db;
    public Servant(int port) {
        super(port);
        try (final Options options = new Options().setCreateIfMissing(true)) {
            this.db = RocksDB.open(options, String.format("/tmp/db/%d", port));
        } catch (RocksDBException e) {
            System.err.printf("Unable to open RocksDB on %d\n", port);
            e.printStackTrace();
            this.db.close();
        }
    }

    protected Handler handleGet() {
        try {
            return ctx -> {
                String key = ctx.pathParam("key");
                try {
                    final byte[] value = this.db.get(key.getBytes());
                    if (value == null) {
                        ctx.result("");
                        ctx.status(404);
                    } else {
                        ctx.result(value);
                        ctx.status(200);
                    }
                } catch (RocksDBException e) {
                    ctx.result("");
                    ctx.status(500);
                }
            };
        } catch (Exception e) {
            System.err.println("Unable to create get handler");
            return null;
        }
    }

    protected Handler handlePost() {
        try {
            return ctx -> {
                final byte[] key = ctx.pathParam("key").getBytes();
                final byte[] value = ctx.body().getBytes();
                try {
                    this.db.put(key, value);
                    ctx.result(value);
                    ctx.status(200);
                } catch (RocksDBException e) {
                    ctx.result("");
                    ctx.status(500);
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
                try {
                    this.db.delete(key.getBytes());
                    ctx.status(200);
                } catch (RocksDBException e) {
                    ctx.result("");
                    ctx.status(500);
                }
            };
        } catch (Exception e) {
            System.err.println("Unable to create get handler");
            return null;
        }
    }
}
