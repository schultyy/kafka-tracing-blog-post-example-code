use kafka::consumer::{Consumer, FetchOffset};
use tracing::{instrument, info};
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use opentelemetry::sdk::{
    trace::{self, RandomIdGenerator, Sampler},
    Resource,
};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[instrument]
fn consume_topic(consumer: &mut Consumer) {
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                let str = String::from_utf8_lossy(m.value);
                info!(payload=str.to_string(), "Consumed");
            }
            let _ = consumer.consume_messageset(ms);
        }
        consumer.commit_consumed().unwrap();
        break;
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
    init_tracer("kafka_consumer", "http://localhost:4317".into())?;
    let mut consumer =
    Consumer::from_hosts(vec!("localhost:9092".to_owned()))
        .with_topic("hnstories".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .create()
        .unwrap();

    consume_topic(&mut consumer);
    println!("Finished");

    global::shutdown_tracer_provider(); // export remaining spans
    Ok(())
}
