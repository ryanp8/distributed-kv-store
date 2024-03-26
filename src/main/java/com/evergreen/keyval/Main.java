package com.evergreen.keyval;

import org.rocksdb.RocksDB;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        RocksDB.loadLibrary();
        new Node(args[0], Integer.parseInt(args[1]), Arrays.copyOfRange(args, 2, args.length));
    }
}