use std::sync::Arc;

use tokio::time::Instant;
use tracing::warn;

use super::{Command, CommandError, Result};
use crate::{
    cluster::{partition::Partition, Cluster, Node},
    net::Connection,
    policy::Policy,
    Key,
};

pub struct SingleCommand<'a> {
    cluster: Arc<Cluster>,
    pub key: &'a Key,
    partition: Partition<'a>,
}

impl<'a> SingleCommand<'a> {
    pub fn new(cluster: Arc<Cluster>, key: &'a Key) -> Self {
        let partition = Partition::new_by_key(key);
        SingleCommand {
            cluster,
            key,
            partition,
        }
    }

    pub async fn get_node(&self) -> Option<Arc<Node>> {
        self.cluster.get_node(&self.partition).await
    }

    pub async fn empty_socket(conn: &mut Connection, receive_size: usize) -> Result<()> {
        // There should not be any more bytes.
        // Empty the socket to be safe.
        if receive_size > 0 {
            conn.read_buffer(receive_size).await?;
        }

        Ok(())
    }

    pub(super) async fn execute(policy: &impl Policy, cmd: &mut impl Command) -> Result<()> {
        let mut iterations = 0;

        // set timeout outside the loop
        let deadline = policy.deadline();

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            iterations += 1;

            // Sleep before trying again, after the first iteration
            if iterations > 1 {
                if let Some(sleep_between_retries) = policy.sleep_between_retries() {
                    tokio::time::sleep(sleep_between_retries).await;
                } else {
                    // yield to free space for the runtime to execute other futures between runs
                    // because the loop would block the thread
                    tokio::task::yield_now().await;
                }
            }

            // check for command timeout
            if let Some(deadline) = deadline {
                if Instant::now() > deadline {
                    break;
                }
            }

            // set command node, so when you return a record it has the node
            let node_future = cmd.get_node();
            let node = match node_future.await {
                Some(node) => node,
                None => continue, // Node is currently inactive. Retry.
            };

            let mut conn = match node.get_connection().await {
                Ok(conn) => conn,
                Err(err) => {
                    warn!(?node, %err, "Node");
                    continue;
                }
            };

            cmd.prepare_buffer(&mut conn)
                .map_err(|e| CommandError::PrepareBuffer(Box::new(e)))?;

            // Send command.
            if let Err(err) = conn.flush().await {
                // IO errors are considered temporary anomalies. Retry.
                // Close socket to flush out possible garbage. Do not put back in pool.
                conn.close().await;
                warn!(?node, %err, "Node");
                continue;
            }

            // Parse results.
            if let Err(err) = cmd.parse_result(&mut conn).await {
                // close the connection
                // cancelling/closing the batch/multi commands will return an error, which will
                // close the connection to throw away its data and signal the server about the
                // situation. We will not put back the connection in the buffer.
                if !super::keep_connection(&err) {
                    conn.close().await;
                }
                return Err(err);
            }

            // command has completed successfully.  Exit method.
            return Ok(());
        }

        Err(CommandError::Timeout)
    }
}
