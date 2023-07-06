Simple consumer with manual offset storing on the server
---

Offsets can also be stored server side.

Have a look https://blog.rabbitmq.com/posts/2021/09/rabbitmq-streams-offset-tracking/ for further info.

query_offset and store_offset methods of consumer can be used to store and retrieve offset from the server.
They require a subscriber_id in input.
