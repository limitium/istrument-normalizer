### KScore

### Topic

### Topology

#### Stateful

#### Stateless

### State

#### Wrappers

### Downstreams

#### Models

#### Stores

#### BookingRequest

#### BookingRequestModel

#### Actions

All actions must be copartitioned with downstream's store.

###### Override 

Overrides RequestData values

`{app_name}.downstream-{downstream_name}-override`

Key: long - referenceId

Value: RequestData

###### Resend

Two types of resend:

* Technical retry - duplicate previous BookingRequest
* Business resend - resend BookingRequest according to downstream's BookingModel 

`{app_name}.downstream-{downstream_name}-resend`

Key: long - referenceId

Value: String - `RESEND_MODEL` if set to "RESEND" then will be `business resend` otherwise - `retry`

###### Terminate

Transits BookingRequest to `TERMINATE` state

`{app_name}.downstream-{downstream_name}-terminate`

Key: long - referenceId

Value: long - bookingRequestId

###### Force ack

Transits BookingRequest to `ACK` state

`{app_name}.downstream-{downstream_name}-forceack`

Key: long - referenceId

Value: long - bookingRequestId