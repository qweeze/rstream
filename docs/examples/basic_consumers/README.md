Simple consumers
---

This section shows basic examples on how to consume messages from a stream.

basic_consumer.py: Basic consumer, offset is not specified so it will start consuming from the very first message
basic_consumer_offset_next.py: We also specify an offset as next so we will start consuming from the next message produced
basic_consumer_offset_offset.py: We also specify a numeric offset from where we want to start consuming.

The subscribe method of consumer will take in input a callback (on_message) that will receive asynchronously the received message.
MessageContext store some info about the message (the stream, and the current offset)

Possible values of OffsetType are: FIRST (default), NEXT, LAST, TIMESTAMP and OFFSET
