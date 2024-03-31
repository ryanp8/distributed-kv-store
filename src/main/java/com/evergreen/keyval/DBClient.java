package com.evergreen.keyval;


import io.javalin.http.Handler;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Options;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;

public class DBClient {

    private RocksDB db;
    private MessageDigest md;
    public DBClient(int port) {
        try (final Options options = new Options().setCreateIfMissing(true)) {
            this.db = RocksDB.open(options, String.format("/tmp/db/%d", port));
        } catch (RocksDBException e) {
            System.err.printf("Unable to open RocksDB on %d\n", port);
            e.printStackTrace();
            this.db.close();
        }

        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (java.security.NoSuchAlgorithmException e) {
            e.printStackTrace();
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

    public HashMap<String, String> lowerBoundGet(long lowerBound) {
        final RocksIterator iter = this.db.newIterator();
        iter.seekToFirst();

        HashMap<String, String> result = new HashMap<>();
        while (iter.isValid()) {
            byte[] key = iter.key();
            byte[] digest = this.md.digest(key);
            if (ByteBuffer.wrap(digest).getLong() > lowerBound) {
                String keyString = new String(key);
                String val = new String(iter.value());
                result.put(keyString, val);
            }
            iter.next();
        }
        return result;
    }

    public HashMap<String, String> upperBoundGet(long upperBound) {
        final RocksIterator iter = this.db.newIterator();
        iter.seekToFirst();

        HashMap<String, String> result = new HashMap<>();
        while (iter.isValid()) {
            byte[] key = iter.key();
            byte[] digest = this.md.digest(key);
            if (ByteBuffer.wrap(digest).getLong() < upperBound) {
                String keyString = new String(key);
                String val = new String(iter.value());
                result.put(keyString, val);
            }
            iter.next();
        }
        return result;
    }

    public void lowerBoundDelete(long lowerBound) {
        final RocksIterator iter = this.db.newIterator();
        iter.seekToFirst();

        ArrayList<byte[]> keys = new ArrayList<>();
        while (iter.isValid()) {
            byte[] key = iter.key();
            byte[] digest = this.md.digest(key);
            if (ByteBuffer.wrap(digest).getLong() > lowerBound) {
                keys.add(key);

            }
            iter.next();
        }
        for (byte[] key : keys) {
            try {
                this.db.delete(key);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }

    public void upperBoundDelete(long upperBound) {
        final RocksIterator iter = this.db.newIterator();
        iter.seekToFirst();

        ArrayList<byte[]> keys = new ArrayList<>();
        while (iter.isValid()) {
            byte[] key = iter.key();
            byte[] digest = this.md.digest(key);
            if (ByteBuffer.wrap(digest).getLong() < upperBound) {
                keys.add(key);

            }
            iter.next();
        }
        for (byte[] key : keys) {
            try {
                this.db.delete(key);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
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
