# Missing parts

These parts are prerequisites to the current proposal.

* Kafka broker
* DAO access to instrument data via https://jdbi.org/
* Standalone TE engine extracted from an axiom platform
* gRPC servers for DAO and TEE coverage with unified protobuf protocols and caching system
  

# Performance

Current approach can be scaled horizontally with partitions based on instrument. Kafka claims almost no limitation on throughput.

# Cons

* No prod expirence with kafka
* POC like solution at the beging
* Lack of overal control and visibility for whole `Instrument normalization` project
* Messages with unresolved instrument are sent to downstream instead of errors consumer
* Leakage of axiom generated messages to the project
* No new technologies:
  * In memory data grid like Apache Ignite
  * K-V storage like ETCD
  * Streaming engine like Flink
  * Container based infrastructure like k8s 


# Pros
  
* Doable
* Modern streaming processing approach
* Introducing kafka broker
* Cloud principles in appliaction architecture
  * Kafka streams based components
  * Possible scaling with partititons
* Processing engine agnostic new services:
  * gRPC TE engine
  * gRPC Instruments


# Possible improvements

* Clean border with axiom stuff - more components
* Web-based control cockpit for project - more components
  