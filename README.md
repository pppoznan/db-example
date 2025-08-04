# Key-Value Database Simulation

This project is a simulation of a key-value database storage system implemented in Java. It demonstrates core concepts of distributed database design, including in-memory caching, persistent storage, and basic replication.

## How to Run the Application

To run this application, you will need a Java Development Kit (JDK) installed (version 21 or higher recommended).

1.  **Build the Java Jar (or use built one in the resources directory of a project) :**

    ```
    mvn package
    ```
    
    The file distributed-db-task-1.0-SNAPSHOT should be in directory target.


3. **Run the application:**
    The application typically consists of a Leader node and one or more Replica nodes. You would usually start the Leader first, then the Replicas. At least one replica is required.

    * **To run the Leader node:**

      ```
      java -jar distributed-db-task-1.0-SNAPSHOT.jar --role LEADER --id 1 --port 27000 --peers 1:127.0.0.1:32001,2:127.0.0.1:32002
      ```
      
    * **To run a Replica node:**

      ```
        java -jar distributed-db-task-1.0-SNAPSHOT.jar --role FOLLOWER --id 2 --port 27001 --sync-port 32001 --peers 1:127.0.0.1:32001,2:127.0.0.1:32002
        java -jar distributed-db-task-1.0-SNAPSHOT.jar --role FOLLOWER --id 3 --port 27002 --sync-port 32002 --peers 1:127.0.0.1:32001,2:127.0.0.1:32002
      ```

4. **Client Interaction:**

   Clients can connect to the Leader node using a simple TCP connection to send commands like `PUT|<key>:<value>`, `GET|<key>`. 
  
   The example Client is put in root of directory.
## Client protocol

    PUT|<key>:<value>
    BATCH_PUT|<key>:<value>,<key>:<value>,...
    GET|<key>
    GET_RANGE|<from>:<to>
    DEL|<key>

## Prerequisites

The following simplifications were made during the development of this simulation:

* **No external libraries**: Do not use any framework nor library as mentioned in the description

* **Key and Data Types**: Keys are assumed to be of type `long`, and data (values) are always `String`.

* **Read Range Order**: The `readRange` operation is guaranteed to return data in ascending order of keys.

* **Replication Protocol**: A simplified TCP custom protocol used for client-node and inter-node communication (Leader to Replicas) to minimize implementation effort.

* **Naive Failover**: In the case of a Leader failure, the replica with the highest port number is chosen as the new leader.

## Architectural Decision Records

### Why Java?

Java was chosen as the implementation language primarily due to daily familiarity and proficiency. This allowed for a quicker development cycle.
Java can outperform different applications because of JIT optimizations. 

### Overhead Considerations

The project acknowledges the overheads associated to Java and used structures, such as:

* **Object Overhead**: The use of Data Transfer Objects (DTOs) and boxed types (e.g., `Long` instead of `long`) introduces additional memory consumption and potential garbage collection pressure.

* **Garbage Collection (GC)**: Frequent object creation and destruction can lead to GC pauses, impacting real-time performance.

* **Streams API**: While convenient, Java Streams can sometimes introduce a performance overhead compared to traditional loop-based processing.

* **Exceptions**: Constructing stack traces for exceptions can be a costly operation.

### Core Algorithms and Data Structures

The database simulation use several algorithms and data structures to achieve its goals:

* **LSM Tree (Log-Structured Merge Tree)**: The core storage engine design is inspired by the LSM tree. This architecture is optimized for write-heavy workloads by buffering writes in memory and asynchronously flushing them to disk, however it uses different approach to persistent storage described below to optimize the read operations.

* **Memtable**: A `TreeMap` is used as the in-memory "Memtable". This provides ordered storage for recent writes, enabling efficient range queries on newly inserted data.

* **LRU Cache (Least Recently Used)**: An LRU cache is implemented to serve frequently accessed data and even cache non-existent keys, reducing disk I/O for hot reads.

* **Persistent Storage**:

    * Data is persisted to files on disk, by background process.

    * A **Hash Index** is maintained for the persistent store to quickly locate files containing specific keys.

    * Each storage file also contains its own **internal index** to further reduce the search time within that specific file. The binary search is used to find the start and end offset of data stored in a file.

    * Files are designed to be mutable, allowing in-place updates.

### Replication and Failover

* **Simple Replication**: Architecture is inspired by Redis cluster architecture (Leader - Follower). Replication from the Leader node to N Replicas is achieved using a custom TCP-based string protocol. This allows replicas to receive updates from the leader. Leader is responsible for Write operations and broadcasting them to all replicas. Replicas serve Read operations.

* **Naive Failover**: A basic failover mechanism is in place where, upon leader failure, the replica with the highest port number is designated as the new leader. This is a simplified approach for demonstration purposes.

## Key Features and How They Are Achieved

### Low Latency Per Item Read or Written

* **Reads**:

    * **LRU Cache**: For frequently accessed keys (and even frequently requested non-existent keys), reads are served directly from the LRU cache, providing very low latency.

    * **Memtable**: Recently written items are immediately available from the in-memory Memtable, ensuring fast access without disk interaction.
    * Mutable persistent storage - instead of many SS Tables with immutable data, we keep always one copy of **key:value** pair which decrease latency for reads.

* **Writes**:

    * **Memtable**: Writes are initially buffered in the Memtable, which is an in-memory operation, offering immediate acknowledgment and low latency.

    * **Asynchronous Persistence**: Data is asynchronously flushed from the Memtable to persistent storage, ensuring that the write path remains fast and non-blocking for client requests.

### High Throughput, Especially When Writing an Incoming Stream of Random Items

* **Memtable**: The use of an in-memory Memtable is crucial for achieving high write throughput. Incoming writes are quickly absorbed into memory, allowing the system to process a large volume of random items without immediately hitting disk I/O bottlenecks.

* **Batch Pruning in `batchPut`**: When performing batch put operations, a pruning mechanism is implemented. This ensures that if the same key is updated multiple times within a single batch, only the *latest* value for that key is retained, minimizing redundant writes to the underlying storage and improving efficiency.

### Ability to Handle Datasets Much Larger Than RAM Without Degradation

* **Persistent Storage with File Differentiation**: The system utilizes persistent storage where data is split into multiple files based on a hash of a "file-differentiator". This allows the dataset to grow beyond the available RAM.

* **File-Specific Indexing**: Each persistent file maintains its own internal index, which helps in quickly locating data within that specific file, reducing the need to scan the entire file.

* **Leader-Replica Scalability**: The current leader-replica architecture can be conceptually extended to a cluster mode. By multiplying this architecture (N leaders, M replicas per leader) and introducing a balancer (similar to Redis Hash Slots), the system could horizontally scale to handle even larger datasets and higher loads.

### Crash Friendliness, Both in Terms of Fast Recovery and Not Losing Data

* **Log File for Recovery**: The application incorporates a log file (write-ahead log file) that records all incoming operations.

* **Recovery on Node Start**: Upon node startup, this log file is consumed and replayed *before* the node is ready to handle messages from clients or the Leader. This ensures that any data written before a crash, but not yet fully persisted, can be recovered, preventing data loss and enabling fast recovery.

### Predictable Behavior Under Heavy Access Load or Large Volume

* **Deterministic Data Structures**: The implementation avoids the use of probabilistic data structures (like Bloom Filters) for core data storage and retrieval, which ensures deterministic behavior and predictable lookup times.

* **Asynchronous Replication**: While the leader sends communication to replicas asynchronously, which might introduce a small delay in replica consistency, the write path for the client remains available, contributing to predictable performance under heavy write loads.

## What Could Be Improved

This simulation serves a simple proof-of-concept. Several areas can be enhanced to improve robustness, performance, and scalability for a production-ready system:

* **Tests**: A significant improvement would be the addition of unit, integration, and performance tests. The current lack of tests is due to the constraint of not using external libraries. 

* **Hexagonal architecture**: To simplify tests, it is worth to consider hexagonal architecture which allows to use in-memory implementation for I/O operations and speedup the process of testing.

* **Connection Pooling**: Implement connection pooling for communication with replicas to reduce the overhead of establishing new TCP connections for every message. Keeping connections IDLE is crucial for performance.

* **Persistent Storage Performance**: As each persistent storage file has to have sorted index, we could optimize the process of sorting data using Counting Sort, as range of keys is known.

* **Protocol Enhancement**: Replace the raw TCP custom protocol with a more robust, well-documented, and efficient protocol like **gRPC**. This would provide better serialization and error handling.

* **Locking Strategy and Race Conditions**: Deeper insights of possible race conditions and refining critical sections is crucial.

* **Scheduler and Thread Pool Tuning (plus Metrics)**: Optimize the configuration of schedulers and thread pools (e.g., for persistence, replication, and client handling) to better match the application's workload characteristics and available hardware resources.

* **Multiple Memtables and Queues**: To further increase write throughput, consider implementing multiple Memtables and a queue system to manage their flushing to disk. This allows concurrent writes to different Memtables.

* **Primitive Types**: Reduce object overhead and GC pressure by using primitive types (e.g., `long`, `byte[]`) instead of boxed types (`Long`) and `String` where possible, especially for internal data representation.

* **Refined File Differentiator**: Improve the "naive file differentiator" logic for persistent storage. A more robust and balanced distribution strategy for files would enhance performance and manageability.

* **Robust Failover Mechanism**: Replace the naive failover mechanism with a distributed consensus algorithm like **Raft**.

* **Replication Reliability**: Implement retry mechanisms for sending data to replicas, and thoroughly consider edge cases such as network timeouts and message ordering guarantees (Inspired by Outbox pattern).

* **Probabilistic Data Structures**: Explore the use of probabilistic data structures like **Counting Bloom Filters (CIBF)**. These could be used for optimizing read paths (e.g., quickly checking for non-existent keys in persistent storage without disk access).

* **Exception handling**: Throwing runtime exceptions without proper handling is a bad practice that should be addressed before reaching the production. Furthermore, building stack trace in Java might be not a cheap operation.

* **Code quality**: Implementation focuses mostly on presenting the architectural overview of the solution. The refactor of code to improve readability and maintainability is required.