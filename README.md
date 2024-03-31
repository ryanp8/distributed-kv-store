# distributed-kv-store

Highly available, network partitioned key-value store inspired by [Amazon's DynamoDB](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) using [RocksDB](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics) and [Javalin](https://github.com/javalin/javalin).
Built to learn more about distributed systems and Java.

## Usage
1. Build the project with Maven `mvn package`
2. For each node you want to start, run `java -cp target/keyval-1.0-SNAPSHOT.jar com.evergreen.keyval.Main [hostname] [port]`

### API
#### Endpoints
1. `GET /db/{key}` Retrieves the value for key in the distributed system
2. `POST /db/{key}` Posts the value in the request body for key in the distributed system (posts to replicas)
3. `DELETE /db/{key}` Deletes the values for key in the distributed system (deletes in replicas)
4. `POST /ring` Adds the recipient server to the ring containing the server provided in the request body
   
#### Internal endpoints
1. `GET /{key}` Retrieves the value for the key in just the target server
2. `POST /{key}` Posts the value for the key in just the target server
3. `DELETE /{key}` Deletes the value for the key in just the target server
4. `GET /keys` Gets all key-value pairs the target server stores.
5. `POST /keys` Posts multiple key-value pairs to the target server. Request body must be a json string.
6. `DELETE /keys` Deletes all key-value pairs the target server stores.
7. `GET /nodes` Retrieves all the nodes in the target server's cluster.
8. `POST /nodes` Updates the nodes in the target server's cluster.

