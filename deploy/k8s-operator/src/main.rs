use barq_operator::controller::{error_policy, reconcile, Context};
use barq_operator::crd::BarqDB;
use futures::StreamExt;
use kube::{client::Client, runtime::Controller, Api};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init Logging
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer().json();
    Registry::default().with(env_filter).with(fmt_layer).init();

    info!("Starting Barq Operator");

    let client = Client::try_default().await?;
    let context = Arc::new(Context {
        client: client.clone(),
    });

    let barqdbs: Api<BarqDB> = Api::all(client);

    Controller::new(barqdbs, Default::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("reconciled {:?}", o),
                Err(e) => error!("reconcile failed: {:?}", e),
            }
        })
        .await;

    Ok(())
}
