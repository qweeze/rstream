Connection broken
---

The client does not support auto-reconnect at the moment.

But it allows you to be notified if such event happens and take action through a callback. 

This callback can be passed to the constructor of (superstream)consumers/(superstream)producers

You can use these callbacks in order to notify your main flow that a disconnection happened to properly close 
consumers and producers or you can use the method reconnect_stream in order to try to reconnect to the stream.

You can find the example in this folder for close and reconnect.

Please note that if you don't specify this callback, just and exception will be raised.

