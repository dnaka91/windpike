use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut, Drop},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::Mutex;

use super::{Connection, Host, NetError, Result};
use crate::policy::ClientPolicy;

#[derive(Debug)]
struct IdleConnection(Connection);

#[derive(Debug)]
struct QueueInternals {
    connections: VecDeque<IdleConnection>,
    num_conns: usize,
}

#[derive(Debug)]
struct SharedQueue {
    internals: Mutex<QueueInternals>,
    capacity: usize,
    host: Host,
    policy: ClientPolicy,
}

#[derive(Debug)]
struct Queue(Arc<SharedQueue>);

impl Queue {
    pub fn with_capacity(capacity: usize, host: Host, policy: ClientPolicy) -> Self {
        let internals = QueueInternals {
            connections: VecDeque::with_capacity(capacity),
            num_conns: 0,
        };
        let shared = SharedQueue {
            internals: Mutex::new(internals),
            capacity,
            host,
            policy,
        };
        Self(Arc::new(shared))
    }

    pub async fn get(&self) -> Result<PooledConnection> {
        let mut internals = self.0.internals.lock().await;
        let connection;
        loop {
            if let Some(IdleConnection(mut conn)) = internals.connections.pop_front() {
                if conn.is_idle() {
                    internals.num_conns -= 1;
                    conn.close().await;
                    continue;
                }
                connection = conn;
                break;
            }

            if internals.num_conns >= self.0.capacity {
                return Err(NetError::NoMoreConnections);
            }

            internals.num_conns += 1;

            // Free the lock to prevent deadlocking
            drop(internals);

            let conn = tokio::time::timeout(
                Duration::from_secs(5),
                Connection::new(&self.0.host.address(), &self.0.policy),
            )
            .await;

            if conn.is_err() {
                let mut internals = self.0.internals.lock().await;
                internals.num_conns -= 1;
                drop(internals);
                return Err(NetError::FailedOpening);
            }

            let conn = conn.unwrap()?;

            connection = conn;
            break;
        }

        Ok(PooledConnection {
            queue: self.clone(),
            conn: Some(connection),
        })
    }

    pub async fn put_back(&self, mut conn: Connection) {
        let mut internals = self.0.internals.lock().await;
        if internals.num_conns < self.0.capacity {
            internals.connections.push_back(IdleConnection(conn));
        } else {
            conn.close().await;
            internals.num_conns -= 1;
        }
    }

    pub async fn drop_conn(&self, mut conn: Connection) {
        {
            let mut internals = self.0.internals.lock().await;
            internals.num_conns -= 1;
        }
        conn.close().await;
    }

    pub async fn clear(&mut self) {
        let mut internals = self.0.internals.lock().await;
        for mut conn in internals.connections.drain(..) {
            conn.0.close().await;
        }
        internals.num_conns = 0;
    }
}

impl Clone for Queue {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

#[derive(Debug)]
pub struct ConnectionPool {
    num_queues: usize,
    queues: Vec<Queue>,
    queue_counter: AtomicUsize,
}

impl ConnectionPool {
    pub fn new(host: &Host, policy: &ClientPolicy) -> Self {
        let num_conns = policy.max_conns_per_node;
        let num_queues = policy.conn_pools_per_node;
        let queues = Self::initialize_queues(num_conns, num_queues, host, policy);
        Self {
            num_queues,
            queues,
            queue_counter: AtomicUsize::default(),
        }
    }

    fn initialize_queues(
        num_conns: usize,
        num_queues: usize,
        host: &Host,
        policy: &ClientPolicy,
    ) -> Vec<Queue> {
        let max = num_conns / num_queues;
        let mut rem = num_conns % num_queues;
        let mut queues = Vec::with_capacity(num_queues);
        for _ in 0..num_queues {
            let mut capacity = max;
            if rem > 0 {
                capacity += 1;
                rem -= 1;
            }
            queues.push(Queue::with_capacity(capacity, host.clone(), policy.clone()));
        }
        queues
    }

    pub async fn get(&self) -> Result<PooledConnection> {
        if self.num_queues == 1 {
            self.queues[0].get().await
        } else {
            let mut attempts = self.num_queues;
            loop {
                let i = self.queue_counter.fetch_add(1, Ordering::Relaxed);
                let connection = self.queues[i % self.num_queues].get().await;
                if matches!(connection, Err(NetError::NoMoreConnections)) {
                    attempts -= 1;
                    if attempts > 0 {
                        continue;
                    }
                }
                return connection;
            }
        }
    }

    pub async fn close(&mut self) {
        for mut queue in self.queues.drain(..) {
            queue.clear().await;
        }
    }
}

#[derive(Debug)]
pub struct PooledConnection {
    queue: Queue,
    pub conn: Option<Connection>,
}

impl PooledConnection {
    pub async fn invalidate(mut self) {
        let conn = self.conn.take().unwrap();
        self.queue.drop_conn(conn).await;
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let queue = self.queue.clone();
            tokio::spawn(async move { queue.put_back(conn).await });
        }
    }
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn.as_ref().unwrap()
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().unwrap()
    }
}
