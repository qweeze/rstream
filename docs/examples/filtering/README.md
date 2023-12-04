Filtering
---
This section includes examples on filtering.

Filtering is a new feature enabled from RabbitMQ 3.13 and it is based on Bloom filter.

https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#filtering 

To Producer and Superstream Producer constructor you can now pass a callback filter_value_extractor.

This callback will assign a filter to every message you send.

On Consumer/Superstream Consumer side you can consumer just messages with a given filter.

During consumer subscribe you can now pass a class FilterConfiguration with three properties:

* values_to_filter: The messages with the filter you want to consume
* predicate: You can apply a predicate for post-filtering in case in the same chunk there are messages with other filters
* match_unfiltered: Default False, if set to True it will consume also messages with no filters.

