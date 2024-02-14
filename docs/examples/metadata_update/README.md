Simple scenarios for Metadata update management
---

If the streams topology changes (ex:Stream deleted or add/remove follower), the client receives a MetadataUpdate event.

The server removes the producers and consumers linked to the stream before sending the Metadata update event.

Similarly to the disconnection scenario the Producer tries to automatically reconnect while the Consumer gets notified by a callback

After this the client can try to reconnect.

Here you can find examples for producers and consumers.

You can start the producer and the consumer and then force the server to change the topology of the stream for example with this command:

rabbitmq-streams delete_replica my-test-stream rabbit@rabbitmqcluster-sample-server-0.rabbitmqcluster-sample-nodes.rabbitmq-system

During the reconnection or the check you could receive the `streamNotAvaiable` error.
`streamNotAvaiable` is a temporary problem. It means that the stream is not ready yet. 

Producer/Superstream-producer side there is nothing really much to do (the Producer tries to reconnect automatically).
Consumer side instead you need to define a callback to catch the event and decide what to do.