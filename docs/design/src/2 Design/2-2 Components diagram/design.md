
Instrument normalization project is devided into several components to reduce code complexity and increase flexiblity. Kafka is used as a transport layer for communication between components. Component is a single threaded application based on Kafka streams library. 

There're stateless and stateful components, all states are exposed to kafka with kafka transaction guarantees. All interaction with external services are idempotent.    

All action steps are almost one to one reflected in the corresponding component.

* Receiver - accepts incoming messages, checks instruments availablity, routes messages 
* Reducer - reduce number of seeker lookups for one instrument, one per 5 mins
* Seeker - performs instrument lookup in 3rd-party static data providers
* Combiner - consumes shelved messages and enriches with lookup instruments, of instrument is not available for 15 mins sends message to dead letter queue
* Router - route messages to downstreams

## Topics

* `gba.raw.messages` - all incoming messages for `GBA`, consumed by `Receiver`
* `gba.instrument.nomalizer.instrument.enriched` - all instrument enriched messages, consumed by `Router`
* `gba.instrument.nomalizer.instrument.missed` - messages with missed instrument, consumed by `Reducer` and `Combiner`
* `gba.instrument.nomalizer.instrument.lookup.requests` - instrument lookup requests, consumed by `Seeker`
* `gba.instrument.nomalizer.instrument.lookup.responses` - instrument lookup responses, consumed by `Combiner`

A component diagram describes the relationships between components and external entities: