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

### Creation of super-stream
The SuperStreamProducer class takes in input a field: super_stream_creation_option of type

```
class SuperStreamCreationOption:
    n_partitions: int
    binding_keys: Optional[list[str]] = None
    arguments: Optional[dict[str, Any]] = None
```

If you want to create simple partitions you can specify the n_partitions field and leave to None the binding_keys field.
Otherwise if you want to create the partitions based on the binding_keys you can specify this field and leave to 0 the n_partitions field
If the super_stream specified in the super_stream field doesn't exist and super_stream_creation_option is set then it will create the super stream.
See: https://github.com/qweeze/rstream/issues/156 for more info