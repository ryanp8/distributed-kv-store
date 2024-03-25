package com.evergreen.keyval;

import org.rocksdb.RocksDB;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        RocksDB.loadLibrary();
        if (args[0].equals("servant")) {
            int port = Integer.parseInt(args[1]);
            Node servant = new Servant(port);
        } else if (args[0].equals("coordinator")) {

            Node coordinator = new Coordinator(8080, Arrays.copyOfRange(args,1, args.length));
        }
    }
}