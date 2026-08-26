use exex_proto::{
    ExExNotification as ProtoExExNotification, SubscribeRequest,
    remote_ex_ex_server::{RemoteExEx, RemoteExExServer},
};
use reth::{builder::NodeTypes, primitives::EthPrimitives, providers::CanonStateSubscriptions};
use reth_exex::{ExExContext, ExExHead, ExExNotification};
use reth_exex_types::serde_bincode_compat::ExExNotification as BincodeExExNotification;
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_tracing::tracing::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, transport::Server};

struct ExExService {
    notifications: Arc<broadcast::Sender<ExExNotification>>,
}

#[tonic::async_trait]
impl RemoteExEx for ExExService {
    type SubscribeStream = ReceiverStream<Result<ProtoExExNotification, Status>>;

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        let mut notifications = self.notifications.subscribe();

        tokio::spawn(async move {
            while let Ok(notification) = notifications.recv().await {
                let bincode_notification: BincodeExExNotification<'_, EthPrimitives> =
                    (&notification).into();
                let data = bincode::serialize(&bincode_notification).expect("failed to serialize");
                let size_mb = data.len() as f64 / (1024.0 * 1024.0);

                let proto_notification = ProtoExExNotification { data };
                if tx.send(Ok(proto_notification)).await.is_err() {
                    info!("Client disconnected");
                    break;
                }
                info!(size_mb = format!("{:.2}", size_mb), "sent to client");
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn remote_exex<Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>>(
    mut ctx: ExExContext<Node>,
    notifications: Arc<broadcast::Sender<ExExNotification>>,
) -> eyre::Result<()> {
    use tokio::select;

    let mut canon_stream = ctx.provider().canonical_state_stream();

    loop {
        select! {

            canon_result = canon_stream.next() => {
                if let Some(canon_notif) = canon_result {
                    let exex_notification = match canon_notif {
                        reth::providers::CanonStateNotification::Commit { new } => {
                            let tip = new.tip().number;
                            info!(target: "reth-rex", block = tip, "canon commit");
                            ExExNotification::ChainCommitted { new }
                        }
                        reth::providers::CanonStateNotification::Reorg { old, new } => {
                            let old_tip = old.tip().number;
                            let new_tip = new.tip().number;
                            info!(target: "reth-rex", old_block = old_tip, new_block = new_tip, "canon reorg");
                            ExExNotification::ChainReorged { old, new }
                        }
                    };
                    let _ = notifications.send(exex_notification);
                }
            }

            exex_result = futures_util::TryStreamExt::try_next(&mut ctx.notifications) => {
                match exex_result {
                    Ok(Some(notification)) => {
                        if let Some(ch) = notification.committed_chain() {
                            ctx.send_finished_height(ch.tip().num_hash())?;
                        }
                    }
                    Ok(None) => {
                        info!(target: "reth-rex", "ExEx notification stream ended");
                        break;
                    }
                    Err(e) => {
                        info!(target: "reth-rex", %e, "ExEx notification stream error");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn main() -> eyre::Result<()> {
    reth::cli::Cli::parse_args().run(|builder, _| async move {
        let notifications = Arc::new(broadcast::channel(32).0);

        let server = Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(300)))
            .http2_keepalive_timeout(Some(Duration::from_secs(60)))
            .add_service(RemoteExExServer::new(ExExService {
                notifications: notifications.clone(),
            }))
            .serve("[::]:10000".parse().unwrap());

        let handle = builder
            .node(EthereumNode::default())
            .install_exex("reth-rex-ext", |mut ctx| async move {
                ctx.set_notifications_with_head(ExExHead::new(ctx.head));
                ctx.send_finished_height(ctx.head)?;
                Ok(remote_exex(ctx, notifications))
            })
            .launch()
            .await?;

        handle
            .node
            .task_executor
            .spawn_critical_task("gRPC server", async move {
                server.await.expect("failed to start gRPC server")
            });

        handle.wait_for_node_exit().await
    })
}
