package com.evergreen.keyval;


import io.javalin.http.Handler;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Options;

public class DBClient {

    private RocksDB db;
    public DBClient(int port) {
        try (final Options options = new Options().setCreateIfMissing(true)) {
            this.db = RocksDB.open(options, String.format("/tmp/db/%d", port));
        } catch (RocksDBException e) {
            System.err.printf("Unable to open RocksDB on %d\n", port);
            e.printStackTrace();
            this.db.close();
        }
    }

    public byte[] get(String key) {
        try {
            return this.db.get(key.getBytes());
        } catch (RocksDBException e) {
            System.err.printf("Unable to get value for key %s\n", key);
            e.printStackTrace();
            return null;
        }
    }

    public byte[] post(String key, String val) {
        try {
            final byte[] valBytes = val.getBytes();
            this.db.put(key.getBytes(), valBytes);
            return valBytes;
        } catch (RocksDBException e) {
            System.err.printf("Unable to post (%s,%s)\n", key, val);
            e.printStackTrace();
            return null;
        }
    }

    public byte[] delete(String key) {
        try {
            final byte[] val = this.db.get(key.getBytes());
            if (val == null) {
                return null;
            }
            this.db.delete(key.getBytes());
            return val;
        } catch (RocksDBException e) {
            System.err.printf("Unable to delete value for key %s\n", key);
            e.printStackTrace();
            return null;
        }
    }
}
