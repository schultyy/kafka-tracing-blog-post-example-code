use opentelemetry::Key;
use opentelemetry::StringValue;
use opentelemetry::trace::Span;
use opentelemetry::trace::Tracer;
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use opentelemetry::{
    sdk::{
        trace::{self, RandomIdGenerator},
        Resource,
    },  KeyValue, 
};
use opentelemetry_otlp::WithExportConfig;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{
        CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
    },
    error::KafkaResult,
    message::Headers,
    ClientConfig, ClientContext, Message, TopicPartitionList,
};
use shared::HeaderExtractor;

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_topic(broker: &str, topic: &str) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", "-1")
        .set("bootstrap.servers", broker)
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("failed to create consumer");

    consumer
        .subscribe(&vec![topic])
        .expect("Can't subscribe to topics");

    println!("Subscribed");

    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        eprintln!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        if let Some(val) = header.value {
                            println!(
                                "  Header {:#?}: {:?}",
                                header.key,
                                String::from_utf8(val.to_vec())
                            );
                        }
                    }
                    let context = global::get_text_map_propagator(|propagator| {
                        propagator.extract(&HeaderExtractor(&headers))
                    });
                    let mut span =
                        global::tracer("consumer").start_with_context("consume_payload", &context);
                    span.set_attribute(KeyValue { key: Key::new("payload"), value: opentelemetry::Value::String(StringValue::from(payload.to_string())) });
                    span.end();
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

pub fn init_tracer(
    application_name: &str,
    collector_endpoint: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let _ = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(collector_endpoint),
        )
        .with_trace_config(
            trace::config()
                .with_id_generator(RandomIdGenerator::default())
                .with_max_events_per_span(64)
                .with_max_attributes_per_span(16)
                .with_max_events_per_span(16)
                .with_resource(Resource::new(vec![KeyValue::new(
                    "service.name",
                    application_name.to_string(),
                )])),
        )
        .install_batch(opentelemetry::runtime::Tokio)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    init_tracer("kafka_consumer", "http://localhost:4317".into())?;

    consume_topic("localhost:9092", "hnstories").await;
    println!("Finished");

    global::shutdown_tracer_provider(); // export remaining spans
    Ok(())
}
