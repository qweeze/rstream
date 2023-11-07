Connection broken
---

The client does not support auto-reconnect at the moment, but it allows you to be notified if such event happens 
and take action through a callback. 

This callback can be passed to the constructor of (superstream)consumers/(superstream)producers during instantiation.

You can use these callbacks in order to notify your main flow that a disconnection happened to properly close 
consumers and producers or you can use the method reconnect_stream in order to try to reconnect to the stream.

You can find the examples in this folder for the close and reconnect scenarios for both (superstream)producers and
(superstream)consumers.

Please note that if you don't specify this callback, just a ConnectionClosed exception is raised.

# Specify an offset to restart

In case of a consumer or super_stream_consumer you can specify in the reconnect_stream method the offset you want to
restart consuming. If you don't specify it by default it reconnects from the last consumed message.

