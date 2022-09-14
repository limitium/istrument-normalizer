`Reducer` - throttles incoming `LookupInstrument` messages in time to prevent hummering of external static provider.

Default settings for `THROTTLE_TIME` will be one `LookupInstrument` per instrument key in 5 mins.

`Reducer` shouldn't be aware about an actual message and works with serialized byte[].

Throttle state should be stored in `InMemoryKeyValueStore` and linked with a topic key (securityId in current case)
