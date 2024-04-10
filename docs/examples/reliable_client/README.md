Complete example of Reliable Client
---

This is an example of hot to use the client in a reliable way. By following these best practices, you can ensure that your
application will be able to recover from network failures, metadata updates and other issues.

The client take in input some parameters listed in appsettings.json like connection parameters, how many streams, if we want 
to run the example in stream and super-stream mode and the possibility to use a load_balancer.

The example uses the asynchronous send() with the usage of the confirmation callback in order to check for confirmations.

### Disconnection management.

Currently the client supports auto-reconnect just for Producer and SuperstreamProducer.
The client does not support auto-reconnect at consumer side at the moment, but it allows you to be notified if such event happens and take action through a callback.
This callback can be passed to the constructor of (superstream)consumers during instantiation.
You can use these callbacks in order to notify your main flow that a disconnection happened to properly close consumers and producers or you can use the method reconnect_stream in order to try to reconnect to the stream.

As you can see in the example the Consumer and Superstream consumers take a on_close_handler callback which inside it is
checking the stream which has been disconnected and then using the reconnect_stream in order to 

### Metadata Update
If the streams topology changes (ex:Stream deleted or add/remove follower), the client receives a MetadataUpdate event.
The server removes the producers and consumers linked to the stream before sending the Metadata update event.
Similarly to the disconnection scenario the Producer and SuperstreamProducer tries to automatically reconnect while the 
Consumer gets notified by a callback (the same one you can use for Disconnection Management)
After this the client can try to reconnect.

As you can see in the example the Consumer and Superstream consumers take a on_close_handler callback which inside it is
checking the stream which has been disconnected and then using the reconnect_stream in order to