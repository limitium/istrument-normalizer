
Instrument normalization project is devided into several components to reduce code complexity and increase flexiblity. Kafka is used as a transport layer for communication between components. Component is a single threaded application based on Kafka streams library. 

There're stateless and stateful components, all states are exposed to kafka with kafka transaction guarantees. All interaction with external services are idempotent.    

All action steps are almost one to one reflected in the corresponding component.

* Receiver - accepts incoming messages, checks instruments availability, shelves messages with missing instruments and enriches with responses from `Seeker`, of instrument is not available for 15 mins sends notification about missed instrument, routes messages
* Reducer - reduce number of seeker lookups for one instrument, one per 5 mins
* Seeker - performs instrument lookup in 3rd-party static data providers

## Topics

* `gba.upstream.domain.{business-flow}.{event-type}.normalizeInstrument` - incoming Instrument normalization requests from all business flows, consumed by `Receiver`
* `gba.instrument.internal.instrumentMissed` - Instrument normalization requests with missed instrument, consumed by `Reducer`
* `gba.instrument.internal.lookupInstrument` - instrument lookup requests, consumed by `Seeker`
* `gba.instrument.internal.instrumentLookuped` - instrument lookup responses, consumed by `Receiver`

A component diagram describes the relationships between components and external entities: