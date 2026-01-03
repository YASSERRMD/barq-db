use crate::crd::BarqDB;
use crate::error::Error;
use crate::error::Result;
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, SecretKeySelector, PodSpec, PodTemplateSpec, ResourceRequirements,
    Service, ServicePort, ServiceSpec, PersistentVolumeClaim, PersistentVolumeClaimSpec, VolumeResourceRequirements,
    VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::{
    api::{Api, Patch, PatchParams, ResourceExt},
    client::Client,
    runtime::controller::Action,
    Resource,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{info, warn};

pub struct Context {
    pub client: Client,
}

pub async fn reconcile(barq: Arc<BarqDB>, ctx: Arc<Context>) -> Result<Action> {
    let client = ctx.client.clone();
    let ns = barq.namespace().unwrap_or("default".into());
    let name = barq.name_any();

    info!("Reconciling BarqDB \"{}\" in \"{}\"", name, ns);

    // Ensure StatefulSet exists
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns);
    let sts = define_statefulset(&barq)?;
    apply_resource(&sts_api, &name, sts).await?;

    // Ensure Service exists
    let svc_api: Api<Service> = Api::namespaced(client.clone(), &ns);
    let svc = define_service(&barq)?;
    apply_resource(&svc_api, &name, svc).await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

pub fn error_policy(_barq: Arc<BarqDB>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!("Reconcile failed: {:?}", error);
    Action::requeue(Duration::from_secs(60))
}

async fn apply_resource<T>(api: &Api<T>, name: &str, resource: T) -> Result<()>
where
    T: std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + Clone + kube::Resource,
{
    let params = PatchParams::apply("barq-operator");
    let patch = Patch::Apply(resource);
    api.patch(name, &params, &patch).await.map_err(Error::KubeError)?;
    Ok(())
}

fn define_statefulset(barq: &BarqDB) -> Result<StatefulSet> {
    let name = barq.name_any();
    let spec = &barq.spec;

    let mut labels = BTreeMap::new();
    labels.insert("app".to_string(), "barq-db".to_string());
    labels.insert("instance".to_string(), name.clone());

    let mut env = vec![
        EnvVar {
            name: "BARQ_ADDR".to_string(),
            value: Some("0.0.0.0:8080".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "BARQ_LOG_LEVEL".to_string(),
            value: Some(spec.config.log_level.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "BARQ_MODE".to_string(),
            value: Some(spec.config.mode.clone()),
            ..Default::default()
        },
    ];

    // Tiering Configuration
    if spec.tiering.enabled {
        env.push(EnvVar {
            name: "BARQ_TIERING_ENABLED".to_string(),
            value: Some("true".to_string()),
            ..Default::default()
        });

        if let Some(warm) = &spec.tiering.warm_storage {
            env.push(EnvVar {
                name: "BARQ_WARM_TIER_PROVIDER".to_string(),
                value: Some(warm.provider.clone()),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "BARQ_WARM_TIER_BUCKET".to_string(),
                value: Some(warm.bucket.clone()),
                ..Default::default()
            });
            // Inject credentials from secret
            add_tier_credentials(&mut env, &warm.secret_ref, &warm.provider, "WARM");
        }
        
        if let Some(cold) = &spec.tiering.cold_storage {
             env.push(EnvVar {
                name: "BARQ_COLD_TIER_PROVIDER".to_string(),
                value: Some(cold.provider.clone()),
                ..Default::default()
            });
             env.push(EnvVar {
                name: "BARQ_COLD_TIER_BUCKET".to_string(),
                value: Some(cold.bucket.clone()),
                ..Default::default()
            });
            // Inject credentials from secret
            add_tier_credentials(&mut env, &cold.secret_ref, &cold.provider, "COLD");
        }
    }

    // Define volume mounts for data persistence
    let volume_mounts = vec![
        VolumeMount {
            name: "data".to_string(),
            mount_path: "/data".to_string(),
            ..Default::default()
        },
    ];

    let sts = StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            owner_references: Some(vec![barq.controller_owner_ref(&()).unwrap()]),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(spec.replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            service_name: name.clone(), // Headless service name
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "barq-db".to_string(),
                        image: Some(spec.image.clone()),
                        ports: Some(vec![
                            ContainerPort {
                                container_port: 8080,
                                name: Some("http".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                container_port: 50051,
                                name: Some("grpc".to_string()),
                                ..Default::default()
                            },
                        ]),
                        env: Some(env),
                        resources: Some(ResourceRequirements {
                            limits: Some(BTreeMap::from([
                                ("cpu".to_string(), Quantity(spec.resources.limits.cpu.clone())),
                                ("memory".to_string(), Quantity(spec.resources.limits.memory.clone())),
                            ])),
                            requests: Some(BTreeMap::from([
                                ("cpu".to_string(), Quantity(spec.resources.requests.cpu.clone())),
                                ("memory".to_string(), Quantity(spec.resources.requests.memory.clone())),
                            ])),
                            ..Default::default()
                        }),
                        volume_mounts: Some(volume_mounts),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![
                PersistentVolumeClaim {
                    metadata: ObjectMeta {
                        name: Some("data".to_string()),
                        ..Default::default()
                    },
                    spec: Some(PersistentVolumeClaimSpec {
                        access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                        resources: Some(VolumeResourceRequirements {
                             requests: Some(BTreeMap::from([
                                ("storage".to_string(), Quantity(spec.storage.size.clone())),
                            ])),
                            ..Default::default()
                        }),
                        storage_class_name: spec.storage.storage_class_name.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            ]),
            ..Default::default()
        }),
        ..Default::default()
    };
    Ok(sts)
}

fn define_service(barq: &BarqDB) -> Result<Service> {
    let name = barq.name_any();
    
    let mut labels = BTreeMap::new();
    labels.insert("app".to_string(), "barq-db".to_string());
    labels.insert("instance".to_string(), name.clone());

    let svc = Service {
        metadata: ObjectMeta {
            name: Some(name.clone()),
             owner_references: Some(vec![barq.controller_owner_ref(&()).unwrap()]),
             labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(labels),
            ports: Some(vec![
                ServicePort {
                    port: 80,
                    target_port: Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(8080)),
                    name: Some("http".to_string()),
                    ..Default::default()
                },
                ServicePort {
                    port: 50051,
                    target_port: Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(50051)),
                    name: Some("grpc".to_string()),
                    ..Default::default()
                },
            ]),
            type_: Some("ClusterIP".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };
    Ok(svc)
}

/// Helper to inject cloud provider credentials from a Kubernetes Secret.
/// Expects secrets with keys like `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` for S3,
/// or `GOOGLE_APPLICATION_CREDENTIALS_JSON` for GCS.
fn add_tier_credentials(env: &mut Vec<EnvVar>, secret_name: &str, provider: &str, _tier_prefix: &str) {
    match provider.to_lowercase().as_str() {
        "s3" => {
            env.push(env_from_secret(
                "AWS_ACCESS_KEY_ID",
                secret_name,
                "AWS_ACCESS_KEY_ID",
            ));
            env.push(env_from_secret(
                "AWS_SECRET_ACCESS_KEY",
                secret_name,
                "AWS_SECRET_ACCESS_KEY",
            ));
            // Optionally add region
            env.push(env_from_secret(
                "AWS_REGION",
                secret_name,
                "AWS_REGION",
            ));
        }
        "gcs" => {
            // GCS typically uses a service account JSON file; here we inject as env var
            env.push(env_from_secret(
                "GOOGLE_APPLICATION_CREDENTIALS_JSON",
                secret_name,
                "GOOGLE_APPLICATION_CREDENTIALS_JSON",
            ));
        }
        "azure" => {
            env.push(env_from_secret(
                "AZURE_STORAGE_ACCOUNT",
                secret_name,
                "AZURE_STORAGE_ACCOUNT",
            ));
            env.push(env_from_secret(
                "AZURE_STORAGE_ACCESS_KEY",
                secret_name,
                "AZURE_STORAGE_ACCESS_KEY",
            ));
        }
        _ => {
            // Unknown provider, skip credential injection
        }
    }
}

fn env_from_secret(env_name: &str, secret_name: &str, key: &str) -> EnvVar {
    EnvVar {
        name: env_name.to_string(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: Some(secret_name.to_string()),
                key: key.to_string(),
                optional: Some(true), // Optional to avoid pod crash if secret key is missing
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

