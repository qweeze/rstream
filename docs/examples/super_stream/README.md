Super stream example
---

[Super Streams Documentation](https://www.rabbitmq.com/streams.html#super-streams) for more details.
[Super Streams blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams)


Step 1: Create the super stream:

    $ rabbitmq-streams add_super_stream invoices --partitions 3


Step 2: Run the Super stream producer:

    $ python3 super_stream_producer.py

Step 3: Run the Super stream consumer:

    $ python3 super_stream_consumer.py

