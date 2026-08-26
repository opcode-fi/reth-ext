use exex_proto::{SubscribeRequest, remote_ex_ex_client::RemoteExExClient};
use reth_ethereum_primitives::EthPrimitives;
use reth_exex_types::serde_bincode_compat::ExExNotification as BincodeExExNotification;
use reth_tracing::tracing::{error, info, warn};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tonic::transport::Endpoint;

pub use reth_exex::ExExNotification;

pub type EthBincodeNotification<'a> = BincodeExExNotification<'a, EthPrimitives>;

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

pub type CanonStateReceiver = mpsc::Receiver<ExExNotification>;

pub fn subscribe(
    endpoint: impl Into<String>,
    handle: &tokio::runtime::Handle,
) -> CanonStateReceiver {
    let endpoint = endpoint.into();
    let (tx, rx) = mpsc::channel(64);
    let handle = handle.clone();

    std::thread::Builder::new()
        .name("rex-cons".into())
        .spawn(move || {
            handle.block_on(async move {
                let mut stream = NotificationStream::new(endpoint);
                loop {
                    let notification = stream.next().await;
                    if tx.send(notification).await.is_err() {
                        info!(target: "rex-cons", "Subscriber channel closed, shutting down");
                        break;
                    }
                }
            });
        })
        .expect("Failed to spawn rex-cons thread");

    rx
}

pub struct NotificationStream {
    endpoint: String,
    stream: Option<tonic::Streaming<exex_proto::ExExNotification>>,
    backoff: Duration,
}

impl NotificationStream {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            stream: None,
            backoff: INITIAL_BACKOFF,
        }
    }

    pub async fn next(&mut self) -> ExExNotification {
        loop {
            if self.stream.is_none() {
                self.connect().await;
            }

            if let Some(ref mut stream) = self.stream {
                match stream.message().await {
                    Ok(Some(proto_notification)) => {
                        match bincode::deserialize::<EthBincodeNotification>(
                            &proto_notification.data,
                        ) {
                            Ok(bincode_notification) => {
                                self.backoff = INITIAL_BACKOFF;
                                return bincode_notification.into();
                            }
                            Err(e) => {
                                error!(%e, "Failed to deserialize notification, skipping");
                                continue;
                            }
                        }
                    }
                    Ok(None) => {
                        warn!("Stream closed by server, reconnecting");
                        self.stream = None;
                    }
                    Err(e) => {
                        error!(%e, "Stream error, reconnecting");
                        self.stream = None;
                    }
                }
            }
        }
    }

    async fn connect(&mut self) {
        loop {
            info!(endpoint = %self.endpoint, "Connecting to reth-rex server");

            match self.try_connect().await {
                Ok(stream) => {
                    info!("Connected and subscribed successfully");
                    self.stream = Some(stream);
                    self.backoff = INITIAL_BACKOFF;
                    return;
                }
                Err(e) => {
                    error!(%e, backoff = ?self.backoff, "Connection failed, retrying");
                    sleep(self.backoff).await;
                    self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    async fn try_connect(
        &self,
    ) -> Result<
        tonic::Streaming<exex_proto::ExExNotification>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let endpoint = Endpoint::from_shared(self.endpoint.clone())?
            .http2_keep_alive_interval(Duration::from_secs(300))
            .keep_alive_timeout(Duration::from_secs(60));

        let channel = endpoint.connect().await?;
        let mut client = RemoteExExClient::new(channel)
            .max_encoding_message_size(usize::MAX)
            .max_decoding_message_size(usize::MAX);

        let stream = client.subscribe(SubscribeRequest {}).await?.into_inner();
        Ok(stream)
    }
}
