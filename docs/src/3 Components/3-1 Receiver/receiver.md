This component is an entry point for all incoming messages with non nomalized instrument.

`Receiver` accepts incoming messages, checks the availablity of the instrument in external `instruments services` via `gRPC` call. If the instrument is mature then an incoming message with instrument data is sent to the `Router`. Otherwise, the message is sent to the `Combiner` 

Only statically loaded instruments go to happy path, on-demand loaded go to `combiner` to preserve local ordering