//! HTTP client based on [aws-smithy-http-client](https://crates.io/crates/aws-smithy-http-client)

use core::{fmt, mem, time};
use core::pin::Pin;
use core::future::{self, Future};
use std::borrow::Cow;

pub use object_store::client::{HttpClient, HttpConnector, HttpService, HttpRequest, HttpRequestBody, HttpResponse, HttpResponseBody, HttpError, HttpErrorKind};
pub use aws_smithy_runtime_api::client::http::HttpClient as AwsHttpClient;
pub use aws_smithy_runtime_api::client::http::HttpConnector as AwsHttpConnector;
pub use aws_smithy_runtime_api::client::http::SharedHttpClient as AwsSharedHttpClient;
pub use aws_smithy_runtime_api::client::http::SharedHttpConnector as AwsSharedHttpConnector;
pub use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
pub use aws_smithy_runtime_api::client::auth::{ResolveAuthSchemeOptions, AuthSchemeOptionResolverParams, AuthSchemeId, AuthSchemeOption, AuthSchemeOptionsFuture};

//It is common for ALB to have limited time for idle connections so adjust pool time to the same limit
//Default keep alive is too long to keep this connection alive so there is no sense in keeping it for longer than 3 minutes
const POOL_TIMEOUT: time::Duration = time::Duration::from_secs(60 * 3);

#[derive(Clone)]
///[AwsSmithyHttpConnector] builder
pub struct Builder {
    #[cfg(any(feature = "ring", feature = "aws-lc", feature = "aws-lc-fips"))]
    crypto: aws_smithy_http_client::tls::Provider,
    idle_pool_timeout: time::Duration
}

impl Builder {
    ///Creates default builder with following parameters
    ///- `idle_pool_timeout` - Set to 3 minutes
    pub const fn new() -> Self {
        #[cfg(all(feature = "ring", not(feature = "aws-lc"), not(feature = "aws-lc-fips")))]
        let crypto = aws_smithy_http_client::tls::Provider::Rustls(aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring);
        #[cfg(all(feature = "aws-lc", not(feature = "aws-lc-fips")))]
        let crypto = aws_smithy_http_client::tls::Provider::Rustls(aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc);
        #[cfg(feature = "aws-lc-fips")]
        let crypto = aws_smithy_http_client::tls::Provider::Rustls(aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLcFips);

        Self {
            #[cfg(any(feature = "ring", feature = "aws-lc", feature = "aws-lc-fips"))]
            crypto,
            idle_pool_timeout: POOL_TIMEOUT
        }
    }

    #[cfg(feature = "ring")]
    ///Overrides crypto provider to be ring
    ///
    ///Requires `ring` feature
    ///
    ///This is default choice unless `aws-lc` or `aws-lc-fips` features enabled
    pub const fn with_ring(mut self) -> Self {
        self.crypto = aws_smithy_http_client::tls::Provider::Rustls(aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring);
        self
    }

    #[cfg(feature = "aws-lc")]
    ///Overrides crypto provider to be ring
    ///
    ///Requires `aws-lc` feature
    ///
    ///This is default choice unless `aws-lc-fips` feature enabled
    pub const fn with_aws_lc(mut self) -> Self {
        self.crypto = aws_smithy_http_client::tls::Provider::Rustls(aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc);
        self
    }

    #[cfg(feature = "aws-lc-fips")]
    ///Overrides crypto provider to be ring
    ///
    ///Requires `aws-lc-fips` feature
    ///
    ///This is default choice whenever feature is enabled
    pub const fn with_aws_lc_fips(mut self) -> Self {
        self.crypto = aws_smithy_http_client::tls::Provider::Rustls(aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLcFips);
        self
    }

    ///Sets idle timeout for connection pool.
    ///
    ///Defaults to 3 minutes
    ///
    ///Set to zero to disable timeout which is not recommended
    pub const fn with_idle_pool_timeout(mut self, timeout: time::Duration) -> Self {
        self.idle_pool_timeout = timeout;
        self
    }

    #[cfg(any(feature = "ring", feature = "aws-lc", feature = "aws-lc-fips"))]
    ///Builds new [AwsSmithyHttpConnector] instance.
    ///
    ///Requires `ring` or `aws-lc` or `aws-lc-fips` features
    pub fn create(&self) -> AwsSharedHttpClient {
        let timeout = if self.idle_pool_timeout.is_zero() {
            None
        } else {
            Some(self.idle_pool_timeout)
        };
        aws_smithy_http_client::Builder::new().tls_provider(self.crypto.clone()).pool_idle_timeout(timeout).build_https()
    }
}

#[derive(Clone, Debug)]
///AWS Smithy client implementing [HttpConnector]
pub struct AwsSmithyHttpConnector {
    http: AwsSharedHttpClient,
    components: RuntimeComponents,
}

impl AwsSmithyHttpConnector {
    ///Creates new instance
    pub const fn new(http: AwsSharedHttpClient, components: RuntimeComponents) -> Self {
        Self {
            http,
            components,
        }
    }

    ///Access underlying AWS HTTP client
    pub const fn http_client(&self) -> &AwsSharedHttpClient {
        &self.http
    }
}

impl HttpConnector for AwsSmithyHttpConnector {
    fn connect(&self, options: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
        use aws_smithy_runtime_api::client::http::HttpConnectorSettings;

        let connect_timeout = if let Some(value) = options.get_config_value(&object_store::ClientConfigKey::ConnectTimeout) {
            time::Duration::from_secs(value.parse().unwrap_or(10))
        } else {
            time::Duration::from_secs(10)
        };

        let timeout = if let Some(value) = options.get_config_value(&object_store::ClientConfigKey::Timeout) {
            time::Duration::from_secs(value.parse().unwrap_or(30))
        } else {
            time::Duration::from_secs(30)
        };

        let settings = HttpConnectorSettings::builder().read_timeout(timeout).connect_timeout(connect_timeout).build();
        let client = AwsSmithyHttpClient {
            http: self.http.http_connector(&settings, &self.components),
        };

        Ok(HttpClient::new(client))
    }
}

//TODO: remove if https://github.com/apache/arrow-rs-object-store/pull/789 gets merged
#[repr(transparent)]
struct BoxedError(Box<dyn std::error::Error + Send + Sync>);

impl fmt::Debug for BoxedError {
    #[inline]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, fmt)
    }
}

impl fmt::Display for BoxedError {
    #[inline]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, fmt)
    }
}

impl std::error::Error for BoxedError {
    #[inline(always)]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

#[derive(Clone, Debug)]
///AWS Smithy client implementing [HttpClient]
pub struct AwsSmithyHttpClient {
    http: AwsSharedHttpConnector
}

impl HttpService for AwsSmithyHttpClient {
    #[inline]
    fn call<'life0, 'async_trait>(&'life0 self, req: HttpRequest) -> Pin<Box<dyn Future<Output = Result<HttpResponse, HttpError>> + Send + 'async_trait>> where Self: 'async_trait, 'life0: 'async_trait {
        use http_body_util::BodyExt;
        match aws_smithy_runtime_api::http::Request::try_from(req.map(|body| aws_smithy_types::body::SdkBody::from_body_1_x(body))) {
            Ok(req) => Box::pin(async move {
                let resp = AwsHttpConnector::call(&self.http, req).await;
                let mut response = resp.map_err(|error| HttpError::new(HttpErrorKind::Connect, error)).and_then(|response| {
                    response.try_into_http1x().map_err(|error| HttpError::new(HttpErrorKind::Decode, error))
                })?;

                let mut response_body = aws_smithy_types::body::SdkBody::empty();
                mem::swap(response.body_mut(), &mut response_body);

                let collected = response_body.collect().await.map_err(|error| HttpError::new(HttpErrorKind::Decode, BoxedError(error)))?;
                Ok(response.map(|_| HttpResponseBody::from(collected.to_bytes())))
            }),
            Err(error) => Box::pin(future::ready(Err(HttpError::new(HttpErrorKind::Request, error))))
        }
    }
}

#[derive(Debug, Copy, Clone)]
///Dummy implementation of [ResolveAuthSchemeOptions]
pub struct DummyAuth;

impl DummyAuth {
    const AUTH_SCHEMA: AuthSchemeId = AuthSchemeId::new("noAuth");
    const AUTH_SCHEMAS: &'static [AuthSchemeId] = &[Self::AUTH_SCHEMA];
}

impl ResolveAuthSchemeOptions for DummyAuth {
    #[inline(always)]
    fn resolve_auth_scheme_options(&self, _params: &AuthSchemeOptionResolverParams) -> Result<std::borrow::Cow<'_, [AuthSchemeId]>, aws_smithy_runtime_api::box_error::BoxError> {
        Ok(Cow::Borrowed(Self::AUTH_SCHEMAS))
    }
    fn resolve_auth_scheme_options_v2<'a>(&'a self, _params: &'a AuthSchemeOptionResolverParams, _cfg: &'a aws_smithy_types::config_bag::ConfigBag, _runtime_components: &'a RuntimeComponents) -> AuthSchemeOptionsFuture<'a> {
        AuthSchemeOptionsFuture::ready(Ok(vec![Self::AUTH_SCHEMA.into()]))
    }
}
