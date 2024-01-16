Simple scenarios for Metadata update management
---

If the streams topology changes (ex:Stream deleted or add/remove follower), the client receives an MetadataUpdate event.

The client is notified by a callback (similar to what happens for connection_broken scenarios)

After this the client can try to reconnect.

Here you can find examples for producers and consumers.

You can start the producer and the consumer and then force the server to change the topology of the stream for example with this command:

rabbitmq-streams delete_replica my-test-stream rabbit@rabbitmqcluster-sample-server-0.rabbitmqcluster-sample-nodes.rabbitmq-system

The examples need to run in a minimal 3 nodes RabbitMQ cluster.

The server removes the producers and consumers linked to the stream before sending the Metadata update event.
It is up to the user to decide what to do. 
For example:
  - Reconnect to producer/consumer 
  - Close the producer/consumer 
  - Check if the stream still exists and reconnect the producer /consumer

During the reconnection or the check you could receive the `streamNotAvaiable` error.
`streamNotAvaiable` is a temporary problem. It means that the stream is not ready yet. 

There is also an example where you can manage both Metadata Update and disconnections