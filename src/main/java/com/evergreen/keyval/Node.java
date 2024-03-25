package com.evergreen.keyval;

import io.javalin.Javalin;
import io.javalin.http.Handler;

abstract public class Node {
    public Node(int port) {
        Javalin app = Javalin.create()
                .get("/{key}", this.handleGet())
                .post("/{key}", this.handlePost())
                .delete("/{key}", this.handleDelete());
        app.start(port);
    }


    abstract protected Handler handleGet();
    abstract protected Handler handlePost();
    abstract protected Handler handleDelete();

}
