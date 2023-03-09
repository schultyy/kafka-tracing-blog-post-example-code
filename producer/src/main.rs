use opentelemetry::{Key, StringValue};
use opentelemetry::trace::Span;
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};

use opentelemetry::sdk::{
    trace::{self, RandomIdGenerator},
    Resource,
};

use opentelemetry::{
    trace::{TraceContextExt, Tracer},
    Context, KeyValue,
};

use opentelemetry_otlp::WithExportConfig;
use rdkafka::message::{OwnedHeaders, Header};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use shared::hn::HNSearchResult;
use std::time::Duration;

async fn send_to_kafka(host: &str, topic: &str, payload: Vec<HNSearchResult>) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", host)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");


    for hn_search_result in payload.iter() {
        let mut span = global::tracer("producer").start("send_to_kafka");
        span.set_attribute(KeyValue { key: Key::new("title"), value: opentelemetry::Value::String(StringValue::from(hn_search_result.title.to_string())) });
        let context = Context::current_with_span(span);
        let serialized = serde_json::to_string(&hn_search_result).unwrap();

        let mut headers = OwnedHeaders::new().insert(Header { key: "key", value: Some("value") });
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&context, &mut shared::HeaderInjector(&mut headers))
        });

        let delivery_status = producer
            .send(
                FutureRecord::to(&topic.to_string())
                        .key(&format!("Key {}", -1))
                    .headers(headers)
                    .payload(&serialized),
                Duration::from_secs(0)
            )
            .await;
        println!("Delivery Status: {:?}", delivery_status);
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
    init_tracer("kafka_producer", "http://localhost:4317".into())?;
    let _span = global::tracer("producer").start("kafka_produce_messages");

    let stories = shared::hn::fetch_hn_stories("Ruby".into(), 100).await?;
    println!("Fetched {} stories", stories.hits.len());
    send_to_kafka("localhost:9092", "hnstories", stories.hits).await;
    global::shutdown_tracer_provider(); // export remaining spans
    Ok(())
}
