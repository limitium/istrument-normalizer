This component is an entry point for all incoming messages with non nomalized instrument.

`Receiver` accepts incoming Instrument normalize requests, checks the availability of the instrument in external `Instrument keeper` via `gRPC` call.

Statefull component stores messages with missing instrument from `Kafka connect` in a local `KeyValueStore`.

To have a single API for instrument normalization process, all upstream components must wrap their business message into `NormalizeInstrument` request

```flatbuffers

table NormalizeInstrument {
    security_id:            string;
    original_message_class: string; //Fully qualified class name of Business message
    original_message:       [byte]; //Serialized business message, _must_ have mutateInstrumentId(long)
    egress_topic:           string;
}

```

`Receiver` creates `LookupInstrument` requests to `Seeker`

```flatbuffers

table LookupInstrument {
    security_id:           string;
    currency:             string;
}

```
