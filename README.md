# kafka-tracing-blog-post-example-code
Example code for Kafka Tracing with OpenTelemetry

## Usage

Start all containers

```bash
$ docker-compose up -d
```

Create a Kafka Topic:

```bash
$ docker exec broker \
   kafka-topics --bootstrap-server broker:9092 \
                --create \
                --topic hnstories
Created topic hnstories.
```

Run the consumer: 

```
$ cargo run -p consumer
Subscribed
Pre rebalance Assign(TPL {hnstories/0: offset=Invalid metadata=""})
Post rebalance Assign(TPL {hnstories/0: offset=Invalid metadata=""})
```
(Wait until you see the output above - might take 15-20sec)

In a second terminal window, run the producer:

```bash
$ cargo run -p producer
```

[Open Jaeger](http://localhost:16686)
