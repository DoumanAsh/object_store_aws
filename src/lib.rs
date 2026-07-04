#![doc = include_str!("../README.md")]

use std::sync::Arc;
use core::fmt;
use core::pin::Pin;
use core::future::Future;

pub use object_store;
pub use object_store::aws::AmazonS3Builder;
pub use aws_config;
pub use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::ProvideCredentials;
use tokio::sync::RwLock;

pub mod http;

#[derive(Debug)]
///Credential errors
pub enum Error {
    ///Credential provider is not available with current config
    MissingCredentials,
    ///Config is missing AWS region
    MissingRegion,
    ///Unable to laod credentials
    CredentialsError(CredentialsError),
}

impl fmt::Display for Error {
    #[inline]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCredentials => fmt.write_str("Credential provider is not available with current AWS config"),
            Self::MissingRegion => fmt.write_str("AWS Config is missing region"),
            Self::CredentialsError(error) => match std::error::Error::source(&error) {
                Some(source) => fmt.write_fmt(format_args!("AWS error getting credentials: {error}({source})")),
                None => fmt.write_fmt(format_args!("AWS error getting credentials: {error}")),
            }
        }
    }
}

impl std::error::Error for Error {
    #[inline]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CredentialsError(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug)]
///AWS credentials provided by [aws-config](https://docs.rs/aws-config/1.8.18/aws_config/struct.SdkConfig.html#method.credentials_provider)
pub struct AwsCredentials {
    region: aws_config::Region,
    provider: aws_credential_types::provider::SharedCredentialsProvider,
    creds: RwLock<aws_credential_types::Credentials>,
    config: aws_config::SdkConfig,
}

impl AwsCredentials {
    #[inline]
    ///Returns region configured during credentials initialization
    pub const fn region(&self) -> &aws_config::Region {
        &self.region
    }

    #[inline]
    ///Returns region configured during credentials initialization
    pub fn region_str(&self) -> &str {
        self.region.as_ref()
    }

    #[inline]
    ///Access underlying AWS SDK config used to initialize credentials provider
    pub const fn config(&self) -> &aws_config::SdkConfig {
        &self.config
    }

    ///Creates [http::AwsSharedHttpClient] instance using provider config, if underlying AWS SDK initialized HTTP client
    ///
    ///This can be used to replace default http connector via [AmazonS3Builder]
    pub fn http_client(&self) -> Result<Option<http::AwsSmithyHttpConnector>, impl std::error::Error + Send + Sync + 'static> {
        use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;

        let http_client = self.config.http_client().clone();
        RuntimeComponents::builder("object-store").with_time_source(self.config.time_source())
                                                  .with_sleep_impl(self.config.sleep_impl())
                                                  .build()
                                                  .map(|components| http_client.map(|http_client| http::AwsSmithyHttpConnector::new(http_client, components)))

    }
}

impl object_store::CredentialProvider for AwsCredentials {
    type Credential = object_store::aws::AwsCredential;
    fn get_credential<'life0,'async_trait>(&'life0 self) -> Pin<Box<dyn Future<Output = object_store::Result<Arc<Self::Credential>>> + Send + 'async_trait>> where 'life0: 'async_trait, Self:'async_trait {
        Box::pin(async {
            let current_creds = self.creds.read().await;
            let creds = if current_creds.expiry().and_then(|expiry| expiry.elapsed().ok()).is_some() {
                //If credentials expired, allow to refresh it
                drop(current_creds);
                match self.provider.provide_credentials().await {
                    Ok(creds) => {
                        let result = Self::Credential {
                            key_id: creds.access_key_id().to_owned(),
                            secret_key: creds.secret_access_key().to_owned(),
                            token: creds.session_token().map(|val| val.to_owned())
                        };
                        *self.creds.write().await = creds;
                        result
                    },
                    Err(error) => {
                        return Err(object_store::Error::Generic {
                            store: "S3",
                            source: Box::new(error),
                        })
                    }

                }
            } else {
                Self::Credential {
                    key_id: current_creds.access_key_id().to_owned(),
                    secret_key: current_creds.secret_access_key().to_owned(),
                    token: current_creds.session_token().map(|val| val.to_owned())
                }
            };

            Ok(Arc::new(creds))
        })
    }
}

///Initializes [AwsCredentials] using current environment
///
///Can optionally provider [http::Builder] to initialize configure http client to be used
///
///Note that to use it, you need to enable at least one of following features: `ring`, `aws-lc`, `aws-lc-fips`
pub async fn init(_http_builder: Option<&http::Builder>) -> Result<AwsCredentials, Error> {
    #[cfg(any(feature = "ring", feature = "aws-lc", feature = "aws-lc-fips"))]
    let http_client = _http_builder.map(http::Builder::create);

    let retry_config = aws_config::retry::RetryConfigBuilder::new().mode(aws_config::retry::RetryMode::Standard)
                                                                   .max_attempts(5)
                                                                   .initial_backoff(core::time::Duration::from_millis(500))
                                                                   .max_backoff(core::time::Duration::from_secs(60))
                                                                   .build();
    #[allow(unused_mut)]
    let mut config = aws_config::from_env().behavior_version(aws_config::BehaviorVersion::latest()).retry_config(retry_config);
    #[cfg(any(feature = "ring", feature = "aws-lc", feature = "aws-lc-fips"))]
    if let Some(http_client) = http_client {
        config = config.http_client(http_client);
    }
    let config = config.load().await;

    let (creds, provider) = match config.credentials_provider() {
        Some(provider) => match provider.provide_credentials().await {
            Ok(creds) => (RwLock::new(creds), provider),
            Err(CredentialsError::CredentialsNotLoaded(_)) => return Err(Error::MissingCredentials),
            Err(error) => return Err(Error::CredentialsError(error)),
        },
        None => return Err(Error::MissingCredentials),
    };
    let region = match config.region() {
        Some(region) => region.clone(),
        None => return Err(Error::MissingRegion),
    };

    Ok(AwsCredentials {
        config,
        region,
        creds,
        provider,
    })
}
