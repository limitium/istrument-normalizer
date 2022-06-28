Statefull component stores messages with missing instrument from `Receiver` and resolved instruments from `Seeker` in local `kafka stream stores`. On each message combines original messages and resolved instruments from `kafka stream store`, if instrumnet isn't resolved sends originam messages to dead message queue.

