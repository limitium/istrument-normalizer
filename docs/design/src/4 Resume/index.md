# Missing parts

These parts are prerequisites to the current proposal.

* Kafka broker
* Containerized computation environment
* External gRPC servers for instrument static data

# Performance

Current approach can be scaled horizontally with partitions based on instrument. Kafka claims almost no limitation on throughput.

# Cons

* No prod expirence with kafka
* POC like solution at the beging
* Lack of overal control and visibility for whole `Instrument normalization` project
* No new technologies:
  * In memory data grid like Apache Ignite
  * K-V storage like ETCD
  * Streaming engine like Flink


# Pros
  
* Doable
* Modern streaming processing approach
* Introducing kafka broker
* Cloud principles in appliaction architecture
  * Kafka streams based components
  * Possible scaling with partititons
* Processing engine agnostic new services:
  * gRPC Instruments


# Possible improvements

* Telemetry
  * Tracing info
  * Log aggregation
  * Metrics collecting  
* The most probable performance enchantment is a cache layer around gRPC calls
* Web-based control cockpit for project - more components
  