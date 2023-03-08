use crate::hn::HNSearchResult;
use kafka::producer::{Producer, Record, RequiredAcks};
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};


use opentelemetry::sdk::{
    trace::{self, RandomIdGenerator, Sampler},
    Resource,
};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use std::time::Duration;
use tracing::{info, instrument};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod hn;

#[instrument(skip(payload))]
fn send_to_kafka(host: &str, topic: &str, payload: Vec<HNSearchResult>) {
    let mut producer = Producer::from_hosts(vec![host.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();

    for search_result in payload {
        let buffer = serde_json::to_string(&search_result).unwrap();
        info!(search_result = search_result.id, "Serializing Payload");
        producer
            .send(&Record::from_value(topic, buffer.as_bytes()))
            .unwrap();
    }
}

pub fn init_tracer(
    application_name: &str,
    collector_endpoint: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(collector_endpoint),
        )
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
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

    let telemetry = Some(tracing_opentelemetry::layer().with_tracer(tracer));

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "RUST_LOG=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry)
        .init();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    init_tracer("kafka_producer", "http://localhost:4317".into())?;

    let stories = hn::fetch_hn_stories("Ruby".into(), 100).await?;
    println!("Fetched {} stories", stories.hits.len());
    send_to_kafka("localhost:9092", "hnstories", stories.hits);
    global::shutdown_tracer_provider(); // export remaining spans
    Ok(())
}
