Evaluating the producer and consumer throughput via Rest Proxy server by Confluent. Most of the concepts are similar to the description from @Apache-Kafka-Confluent-Evaluation repositry. This uses the Rest proxy service instead of the java client. I would highly recommend reading the http://docs.confluent.io/current/kafka-rest/docs/index.html before using this code for any benchmarking.

Setup
Hardware: Mac Os, 15Gb RAM, ~400gb used/ 512gb disk space, 7 cpu cores 

Producer -- Around 25k - 30K messages per second. When sending 100k messages per topic with message size (10k) .This can go up based on the Http connection with KEEP_ALIVE property. This is still TBD>

Consumer -- 40k-60k messages per second (Based up on the partition split)
