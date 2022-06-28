This component does actual lookup of the instrument in external providers. Depends on the information available in the request, lookup can be confident for a particular provider or uses a "guessing" approach.

If the instrument is found, it will be stored in a `instrument keeper` and published to `downstreams` as a new separate message.
