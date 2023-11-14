Simple producers
---

This section shows basic examples on how to send messages to RabbitMQ streams:

* Using send: with send you can send a message to a RabbitMQ stream. Send is asynchronous. </br>
The library is using a dedicated background thread to send the messages to the RabbitMQ server. </br>
See producer_send.py
* Using send_batch: Send batch is synchronous. You send a batch of messages and wait till they are sent to the server
See producer_send_batch.py
* Using send_wait: send_wait is synchronous and it will block until confirmation from the server have been received.
A timeout value can be specified (default 5s) in the method, after that an exception is raised.

send and send_batch by default don't wait for confirmation. You can enable asynchronous confirmation through callbacks.
You can find examples in producers_with_confirmations folder.
