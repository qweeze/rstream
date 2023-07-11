Super stream example
---

[Super Streams Documentation](https://www.rabbitmq.com/streams.html#super-streams) for more details.
[Super Streams blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams)


Step 1: Create the super stream (routing keys are necessary for binding keys resolution strategy):

    $ rabbitmq-streams add_super_stream invoices --routing-keys=key1,key2,key3


Step 2: Run the Super stream producer (with hash strategy):

    $ python3 sac_super_stream_producer.py

or Run the Super stream producer (with key strategy):

    $ python3 sac_super_stream_producer_key.py

Hashing the routing key to pick a partition is only one way to route messages to the appropriate streams. 
The stream client provides another way to resolve streams, based on the routing key and the bindings between the super stream exchange and the streams.

You can

Step 3: Run the Super stream consumer:

    $ python3 super_stream_consumer.py

