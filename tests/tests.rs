use object_store_aws::object_store::CredentialProvider;
use object_store_aws::{aws_config, AwsCredentials};
use aws_credential_types::Credentials;
use aws_credential_types::provider::{self, ProvideCredentials, SharedCredentialsProvider};

use std::time;
use core::sync::atomic;

#[derive(Debug)]
pub struct TestCredentialStore {
    calls: atomic::AtomicU32,
    expiration: atomic::AtomicU64,
}

impl TestCredentialStore {
    #[inline]
    fn new() -> Self {
        Self {
            calls: atomic::AtomicU32::new(0),
            expiration: atomic::AtomicU64::new(0),
        }
    }

    fn get_expiration(&self) -> Option<time::SystemTime> {
        let expiration = self.expiration.load(atomic::Ordering::Acquire);
        if expiration == 0 {
            None
        } else {
            Some(time::UNIX_EPOCH + time::Duration::from_millis(expiration))
        }
    }
    fn set_expire_unix_timestamp(&self, time: time::Duration) {
        self.expiration.store(time.as_millis() as _, atomic::Ordering::Release);
    }

    #[inline]
    fn calls(&self) -> u32 {
        self.calls.load(atomic::Ordering::Acquire)
    }
}

impl ProvideCredentials for TestCredentialStore {
    fn provide_credentials<'a>(&'a self) -> provider::future::ProvideCredentials<'a> where Self: 'a {
        self.calls.fetch_add(1, atomic::Ordering::AcqRel);
        let mut credentials = Credentials::builder().account_id("test")
                                                    .access_key_id("1")
                                                    .secret_access_key("secret")
                                                    .provider_name("test");
        credentials.set_expiry(self.get_expiration());
        let credentials = credentials.build();
        provider::future::ProvideCredentials::ready(Ok(credentials))
    }
}

fn get_test_credential_store(provider: &SharedCredentialsProvider) -> &TestCredentialStore {
    unsafe {
        &*(provider.as_ref() as *const _ as *const TestCredentialStore)
    }
}

#[tokio::test]
async fn should_verify_credentials_provider() {
    let config = aws_config::ConfigLoader::default().empty_test_environment().region("ap-northeast1").credentials_provider(TestCredentialStore::new()).load().await;
    let provider = config.credentials_provider().expect("to have credential provider");
    let test_store = get_test_credential_store(&provider);
    //put expiration into past
    test_store.set_expire_unix_timestamp(time::Duration::from_secs(1));

    let credentials = AwsCredentials::from_config(config).await.expect("to init aws credentials");
    assert_eq!(test_store.calls(), 1);
    //put expiration into future
    test_store.set_expire_unix_timestamp(time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap() + time::Duration::from_secs(2));
    credentials.get_credential().await.expect("get credential");
    assert_eq!(test_store.calls(), 2);
    credentials.get_credential().await.expect("get credential");
    assert_eq!(test_store.calls(), 2);

    //Await expiration
    std::thread::sleep(time::Duration::from_secs(2));

    credentials.get_credential().await.expect("get credential");
    assert_eq!(test_store.calls(), 3);
}
