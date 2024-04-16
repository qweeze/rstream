Super stream example
---

[Super Streams Documentation](https://www.rabbitmq.com/streams.html#super-streams) for more details.
[Super Streams blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams)


Step 1: Create the super stream (routing keys are necessary for binding keys resolution strategy):

The stream is automatically created by the SuperStreamProducer constructor if it doesn't exist

You can specify the numer of the partitions using this struct (in line 23 of super_stream_producer.py):

super_stream_creation_opt = SuperStreamCreationOption(n_partitions=3)

and passing this struct to the SuperStreamProducer constructor (line 31)

Step 2: Run the Super stream producer (with hash strategy):

    $ python3 super_stream_producer.py

or Run the Super stream producer (with key strategy):

    $ python3 super_stream_producer_key.py

Hashing the routing key to pick a partition is only one way to route messages to the appropriate streams. 
The stream client provides another way to resolve streams, based on the routing key and the bindings between the super stream exchange and the streams.

You can

Step 3: Run the Super stream consumer:

    $ python3 super_stream_consumer.py

