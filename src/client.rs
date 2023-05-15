use std::{fmt::Write, str, sync::Arc, vec::Vec};

use tokio::sync::mpsc;

use crate::{
    batch::BatchExecutor,
    cluster::Cluster,
    commands::{
        CommandError, DeleteCommand, ExistsCommand, OperateCommand, ReadCommand, ScanCommand,
        TouchCommand, WriteCommand,
    },
    errors::{Error, Result},
    index::{CollectionIndexType, IndexTask, IndexType},
    net::ToHosts,
    operations::{Operation, OperationType},
    policy::{BasePolicy, BatchPolicy, ClientPolicy, ScanPolicy, WritePolicy},
    BatchRead, Bin, Bins, Key, Record, RecordSet, ResultCode,
};

/// Instantiate a Client instance to access an Aerospike database cluster and perform database
/// operations.
///
/// The client is thread-safe. Only one client instance should be used per cluster. Multiple
/// threads should share this cluster instance.
///
/// Your application uses this class' API to perform database operations such as writing and
/// reading records, and selecting sets of records. Write operations include specialized
/// functionality such as append/prepend and arithmetic addition.
///
/// Each record may have multiple bins, unless the Aerospike server nodes are configured as
/// "single-bin". In "multi-bin" mode, partial records may be written or read by specifying the
/// relevant subset of bins.
#[derive(Clone, Debug)]
pub struct Client {
    cluster: Arc<Cluster>,
}

impl Client {
    /// Initializes Aerospike client with suitable hosts to seed the cluster map. The client policy
    /// is used to set defaults and size internal data structures. For each host connection that
    /// succeeds, the client will:
    ///
    /// - Add host to the cluster map
    /// - Request host's list of other nodes in cluster
    /// - Add these nodes to the cluster map
    ///
    /// In most cases, only one host is necessary to seed the cluster. The remaining hosts are
    /// added as future seeds in case of a complete network failure.
    ///
    /// If one connection succeeds, the client is ready to process database requests. If all
    /// connections fail and the policy's `fail_`
    ///
    /// The seed hosts to connect to (one or more) can be specified as a comma-separated list of
    /// hostnames or IP addresses with optional port numbers, e.g.
    ///
    /// ```text
    /// 10.0.0.1:3000,10.0.0.2:3000,10.0.0.3:3000
    /// ```
    ///
    /// Port 3000 is used by default if the port number is omitted for any of the hosts.
    ///
    /// # Examples
    ///
    /// Using an environment variable to set the list of seed hosts.
    ///
    /// ```rust
    /// use windpike::{policy::ClientPolicy, Client};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn new(policy: &ClientPolicy, hosts: impl ToHosts) -> Result<Self> {
        let hosts = hosts.to_hosts()?;
        let cluster = Cluster::new(policy.clone(), &hosts).await?;

        Ok(Self { cluster })
    }

    /// Closes the connection to the Aerospike cluster.
    pub fn close(&self) {
        self.cluster.close();
    }

    /// Returns `true` if the client is connected to any cluster nodes.
    pub async fn is_connected(&self) -> bool {
        self.cluster.is_connected().await
    }

    /// Returns a list of the names of the active server nodes in the cluster.
    pub async fn node_names(&self) -> Vec<String> {
        self.cluster
            .nodes()
            .await
            .iter()
            .map(|node| node.name().to_owned())
            .collect()
    }

    /// Read record for the specified key. Depending on the bins value provided, all record bins,
    /// only selected record bins or only the record headers will be returned. The policy can be
    /// used to specify timeouts.
    ///
    /// # Examples
    ///
    /// Fetch specified bins for a record with the given key.
    ///
    /// ```rust
    /// use windpike::{
    ///     errors::CommandError,
    ///     policy::{BasePolicy, ClientPolicy},
    ///     Client, Key, ResultCode,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     let key = Key::new("test", "test", "mykey");
    ///     match client.get(&BasePolicy::default(), &key, ["a", "b"]).await {
    ///         Ok(record) => println!("a={:?}", record.bins.get("a")),
    ///         Err(CommandError::ServerError(ResultCode::KeyNotFoundError)) => {
    ///             println!("No such record: {key:?}")
    ///         }
    ///         Err(err) => println!("Error fetching record: {err}"),
    ///     }
    /// }
    /// ```
    ///
    /// Determine the remaining time-to-live of a record.
    ///
    /// ```rust
    /// use windpike::{
    ///     errors::CommandError,
    ///     policy::{BasePolicy, ClientPolicy},
    ///     Bins, Client, Key, ResultCode,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     let key = Key::new("test", "test", "mykey");
    ///     match client.get(&BasePolicy::default(), &key, Bins::None).await {
    ///         Ok(record) => match record.time_to_live() {
    ///             None => println!("record never expires"),
    ///             Some(duration) => println!("ttl: {} secs", duration.as_secs()),
    ///         },
    ///         Err(CommandError::ServerError(ResultCode::KeyNotFoundError)) => {
    ///             println!("No such record: {key:?}")
    ///         }
    ///         Err(err) => println!("Error fetching record: {err}"),
    ///     }
    /// }
    /// ```
    ///
    /// # Panics
    /// Panics if the return is invalid
    pub async fn get<T>(
        &self,
        policy: &BasePolicy,
        key: &Key,
        bins: T,
    ) -> Result<Record, CommandError>
    where
        T: Into<Bins> + Send + Sync + 'static,
    {
        let bins = bins.into();
        let mut command = ReadCommand::new(policy, Arc::clone(&self.cluster), key, bins);
        command.execute().await?;
        Ok(command.record.unwrap())
    }

    /// Read multiple record for specified batch keys in one batch call. This method allows
    /// different namespaces/bins to be requested for each key in the batch. If the `BatchRead` key
    /// field is not found, the corresponding record field will be `None`. The policy can be used
    /// to specify timeouts and maximum concurrent threads. This method requires Aerospike Server
    /// version >= 3.6.0.
    ///
    /// # Examples
    ///
    /// Fetch multiple records in a single client request
    ///
    /// ```rust
    /// use windpike::{
    ///     policy::{BatchPolicy, ClientPolicy},
    ///     BatchRead, Bins, Client, Key,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     let bins = Bins::from(["name", "age"]);
    ///     let mut batch_reads = vec![];
    ///     for i in 0..10 {
    ///         let key = Key::new("test", "test", i);
    ///         batch_reads.push(BatchRead::new(key, bins.clone()));
    ///     }
    ///     match client.batch_get(&BatchPolicy::default(), batch_reads).await {
    ///         Ok(results) => {
    ///             for result in results {
    ///                 match result.record {
    ///                     Some(record) => println!("{:?} => {:?}", result.key, record.bins),
    ///                     None => println!("No such record: {:?}", result.key),
    ///                 }
    ///             }
    ///         }
    ///         Err(err) => println!("Error executing batch request: {err}"),
    ///     }
    /// }
    /// ```
    pub async fn batch_get(
        &self,
        policy: &BatchPolicy,
        batch_reads: Vec<BatchRead>,
    ) -> Result<Vec<BatchRead>> {
        let executor = BatchExecutor::new(Arc::clone(&self.cluster));
        executor.execute_batch_read(policy, batch_reads).await
    }

    /// Write record bin(s). The policy specifies the transaction timeout, record expiration and
    /// how the transaction is handled when the record already exists.
    ///
    /// # Examples
    ///
    /// Write a record with a single integer bin.
    ///
    /// ```rust
    /// use windpike::{
    ///     policy::{ClientPolicy, WritePolicy},
    ///     Bin, Client, Key,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     let key = Key::new("test", "test", "mykey");
    ///     let bin = Bin::new("i", 42);
    ///     match client.put(&WritePolicy::default(), &key, &vec![bin]).await {
    ///         Ok(()) => println!("Record written"),
    ///         Err(err) => println!("Error writing record: {err}"),
    ///     }
    /// }
    /// ```
    ///
    /// Write a record with an expiration of 10 seconds.
    ///
    /// ```rust
    /// use windpike::{
    ///     policy,
    ///     policy::{ClientPolicy, WritePolicy},
    ///     Bin, Client, Key,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     let key = Key::new("test", "test", "mykey");
    ///     let bin = Bin::new("i", 42);
    ///     let mut policy = WritePolicy::default();
    ///     policy.expiration = policy::Expiration::Seconds(10);
    ///     match client.put(&policy, &key, &vec![bin]).await {
    ///         Ok(()) => println!("Record written"),
    ///         Err(err) => println!("Error writing record: {err}"),
    ///     }
    /// }
    /// ```
    pub async fn put<'a, 'b>(
        &self,
        policy: &'a WritePolicy,
        key: &'a Key,
        bins: &'a [Bin<'b>],
    ) -> Result<(), CommandError> {
        let mut command = WriteCommand::new(
            policy,
            Arc::clone(&self.cluster),
            key,
            bins,
            OperationType::Write,
        );
        command.execute().await
    }

    /// Add integer bin values to existing record bin values. The policy specifies the transaction
    /// timeout, record expiration and how the transaction is handled when the record already
    /// exists. This call only works for integer values.
    ///
    /// # Examples
    ///
    /// Add two integer values to two existing bin values.
    ///
    /// ```rust
    /// use windpike::{
    ///     policy::{ClientPolicy, WritePolicy},
    ///     Bin, Client, Key,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     let key = Key::new("test", "test", "mykey");
    ///     let bina = Bin::new("a", 1);
    ///     let binb = Bin::new("b", 2);
    ///     let bins = vec![bina, binb];
    ///     match client.add(&WritePolicy::default(), &key, &bins).await {
    ///         Ok(()) => println!("Record updated"),
    ///         Err(err) => println!("Error writing record: {err}"),
    ///     }
    /// }
    /// ```
    pub async fn add<'a, 'b>(
        &self,
        policy: &'a WritePolicy,
        key: &'a Key,
        bins: &'a [Bin<'b>],
    ) -> Result<(), CommandError> {
        let mut command = WriteCommand::new(
            policy,
            Arc::clone(&self.cluster),
            key,
            bins,
            OperationType::Incr,
        );
        command.execute().await
    }

    /// Append bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub async fn append<'a, 'b>(
        &self,
        policy: &'a WritePolicy,
        key: &'a Key,
        bins: &'a [Bin<'b>],
    ) -> Result<(), CommandError> {
        let mut command = WriteCommand::new(
            policy,
            Arc::clone(&self.cluster),
            key,
            bins,
            OperationType::Append,
        );
        command.execute().await
    }

    /// Prepend bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub async fn prepend<'a, 'b>(
        &self,
        policy: &'a WritePolicy,
        key: &'a Key,
        bins: &'a [Bin<'b>],
    ) -> Result<(), CommandError> {
        let mut command = WriteCommand::new(
            policy,
            Arc::clone(&self.cluster),
            key,
            bins,
            OperationType::Prepend,
        );
        command.execute().await
    }

    /// Delete record for specified key. The policy specifies the transaction timeout.
    /// The call returns `true` if the record existed on the server before deletion.
    ///
    /// # Examples
    ///
    /// Delete a record.
    ///
    /// ```rust
    /// use windpike::{
    ///     policy::{ClientPolicy, WritePolicy},
    ///     Client, Key,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     let key = Key::new("test", "test", "mykey");
    ///     match client.delete(&WritePolicy::default(), &key).await {
    ///         Ok(true) => println!("Record deleted"),
    ///         Ok(false) => println!("Record did not exist"),
    ///         Err(err) => println!("Error deleting record: {err}"),
    ///     }
    /// }
    /// ```
    pub async fn delete(&self, policy: &WritePolicy, key: &Key) -> Result<bool, CommandError> {
        let mut command = DeleteCommand::new(policy, Arc::clone(&self.cluster), key);
        command.execute().await?;
        Ok(command.existed)
    }

    /// Reset record's time to expiration using the policy's expiration. Fail if the record does
    /// not exist.
    ///
    /// # Examples
    ///
    /// Reset a record's time to expiration to the default ttl for the namespace.
    ///
    /// ```rust
    /// use windpike::{
    ///     policy,
    ///     policy::{ClientPolicy, WritePolicy},
    ///     Client, Key,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     let key = Key::new("test", "test", "mykey");
    ///     let mut policy = WritePolicy::default();
    ///     policy.expiration = policy::Expiration::NamespaceDefault;
    ///     match client.touch(&policy, &key).await {
    ///         Ok(()) => println!("Record expiration updated"),
    ///         Err(err) => println!("Error writing record: {}", err),
    ///     }
    /// }
    /// ```
    pub async fn touch(&self, policy: &WritePolicy, key: &Key) -> Result<(), CommandError> {
        let mut command = TouchCommand::new(policy, Arc::clone(&self.cluster), key);
        command.execute().await
    }

    /// Determine if a record key exists. The policy can be used to specify timeouts.
    pub async fn exists(&self, policy: &WritePolicy, key: &Key) -> Result<bool, CommandError> {
        let mut command = ExistsCommand::new(policy, Arc::clone(&self.cluster), key);
        command.execute().await?;
        Ok(command.exists)
    }

    /// Perform multiple read/write operations on a single key in one batch call.
    ///
    /// Operations on scalar values, lists and maps can be performed in the same call.
    ///
    /// Operations execute in the order specified by the client application.
    ///
    /// # Examples
    ///
    /// Add an integer value to an existing record and then read the result, all in one database
    /// call.
    ///
    /// ```rust
    /// use windpike::{
    ///     operations::scalar,
    ///     policy::{ClientPolicy, WritePolicy},
    ///     Bin, Client, Key,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     let key = Key::new("test", "test", "mykey");
    ///     let bin = Bin::new("a", 42);
    ///     let ops = vec![scalar::add(&bin), scalar::get_bin("a")];
    ///     match client.operate(&WritePolicy::default(), &key, &ops).await {
    ///         Ok(record) => println!("The new value is {}", record.bins.get("a").unwrap()),
    ///         Err(err) => println!("Error writing record: {err}"),
    ///     }
    /// }
    /// ```
    /// # Panics
    ///  Panics if the return is invalid
    pub async fn operate(
        &self,
        policy: &WritePolicy,
        key: &Key,
        ops: &[Operation<'_>],
    ) -> Result<Record, CommandError> {
        let mut command = OperateCommand::new(policy, Arc::clone(&self.cluster), key, ops);
        command.execute().await?;
        Ok(command.read_command.record.unwrap())
    }

    /// Read all records in the specified namespace and set and return a record iterator. The scan
    /// executor puts records on a queue in separate threads. The calling thread concurrently pops
    /// records off the queue through the record iterator. Up to `policy.max_concurrent_nodes`
    /// nodes are scanned in parallel. If concurrent nodes is set to zero, the server nodes are
    /// read in series.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use windpike::{
    ///     policy::{ClientPolicy, ScanPolicy},
    ///     Bins, Client,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     match client
    ///         .scan(&ScanPolicy::default(), "test", "demo", Bins::All)
    ///         .await
    ///     {
    ///         Ok(mut records) => {
    ///             let mut count = 0;
    ///             while let Some(record) = records.next().await {
    ///                 match record {
    ///                     Ok(record) => count += 1,
    ///                     Err(err) => panic!("Error executing scan: {err}"),
    ///                 }
    ///             }
    ///             println!("Records: {count}");
    ///         }
    ///         Err(err) => println!("Failed to execute scan: {err}"),
    ///     }
    /// }
    /// ```
    ///
    /// # Panics
    /// Panics if the async block fails
    pub async fn scan<T>(
        &self,
        policy: &ScanPolicy,
        namespace: &str,
        set_name: &str,
        bins: T,
    ) -> Result<RecordSet>
    where
        T: Into<Bins> + Send + Sync + 'static,
    {
        let bins = bins.into();
        let nodes = self.cluster.nodes().await;
        let (queue_tx, queue_rx) = mpsc::channel(policy.record_queue_size);
        let recordset = RecordSet::new(queue_rx);
        let task_id = recordset.task_id();

        for node in nodes {
            let cluster = Arc::clone(&self.cluster);
            let node = Arc::clone(&node);
            let policy = policy.clone();
            let namespace = namespace.to_owned();
            let set_name = set_name.to_owned();
            let bins = bins.clone();
            let queue_tx = queue_tx.clone();

            tokio::spawn(async move {
                let partitions = cluster.node_partitions(&node, &namespace).await;

                ScanCommand::new(
                    &policy, node, &namespace, &set_name, bins, queue_tx, task_id, partitions,
                )
                .execute()
                .await
                .unwrap();
            });
        }
        Ok(recordset)
    }

    /// Removes all records in the specified namespace/set efficiently.
    ///
    /// This method is many orders of magnitude faster than deleting records one at a time. It
    /// requires Aerospike Server version 3.12 or later. See
    /// <https://www.aerospike.com/docs/reference/info#truncate> for further info.
    ///
    /// The `set_name` is optional; set to `""` to delete all sets in `namespace`.
    ///
    /// `before_nanos` optionally specifies a last update timestamp (lut); if it is greater than
    /// zero, only records with a lut less than `before_nanos` are deleted. Units are in
    /// nanoseconds since unix epoch (1970-01-01). Pass in zero to delete all records in the
    /// namespace/set recardless of last update time.
    pub async fn truncate(&self, namespace: &str, set_name: &str, before_nanos: i64) -> Result<()> {
        let mut cmd = String::with_capacity(160);
        cmd.push_str("truncate:namespace=");
        cmd.push_str(namespace);

        if !set_name.is_empty() {
            cmd.push_str(";set=");
            cmd.push_str(set_name);
        }

        if before_nanos > 0 {
            write!(cmd, ";lut={before_nanos}").ok();
        }

        self.send_info_cmd(&cmd)
            .await
            .map_err(|e| Error::Truncate(Box::new(e)))
    }

    /// Create a secondary index on a bin containing scalar values. This asynchronous server call
    /// returns before the command is complete.
    ///
    /// # Examples
    ///
    /// The following example creates an index `idx_foo_bar_baz`. The index is in namespace `foo`
    /// within set `bar` and bin `baz`:
    ///
    /// ```rust
    /// use windpike::{index::IndexType, policy::ClientPolicy, Client};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
    ///         .await
    ///         .unwrap();
    ///
    ///     match client
    ///         .create_index("foo", "bar", "baz", "idx_foo_bar_baz", IndexType::Numeric)
    ///         .await
    ///     {
    ///         Err(err) => println!("Failed to create index: {err}"),
    ///         _ => {}
    ///     }
    /// }
    /// ```
    pub async fn create_index(
        &self,
        namespace: &str,
        set_name: &str,
        bin_name: &str,
        index_name: &str,
        index_type: IndexType,
    ) -> Result<IndexTask> {
        self.create_complex_index(namespace, set_name, bin_name, index_name, index_type, None)
            .await?;
        Ok(IndexTask::new(
            Arc::clone(&self.cluster),
            namespace.to_owned(),
            index_name.to_owned(),
        ))
    }

    /// Create a complex secondary index on a bin containing scalar, list or map values. This
    /// asynchronous server call returns before the command is complete.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_complex_index(
        &self,
        namespace: &str,
        set_name: &str,
        bin_name: &str,
        index_name: &str,
        index_type: IndexType,
        collection_index_type: Option<CollectionIndexType>,
    ) -> Result<()> {
        let cit_str = collection_index_type
            .map(|v| format!("indextype={v};"))
            .unwrap_or_default();
        let cmd = format!(
            "sindex-create:ns={namespace};set={set_name};indexname={index_name};numbins=1;\
             {cit_str}indexdata={bin_name},{index_type};priority=normal",
        );
        self.send_info_cmd(&cmd)
            .await
            .map_err(|e| Error::CreateIndex(Box::new(e)))
    }

    /// Delete secondary index.
    pub async fn drop_index(
        &self,
        namespace: &str,
        set_name: &str,
        index_name: &str,
    ) -> Result<()> {
        let set_name = if set_name.is_empty() {
            String::new()
        } else {
            format!("set={set_name};")
        };
        let cmd = format!("sindex-delete:ns={namespace};{set_name}indexname={index_name}");
        self.send_info_cmd(&cmd)
            .await
            .map_err(|e| Error::Truncate(Box::new(e)))
    }

    async fn send_info_cmd(&self, cmd: &str) -> Result<()> {
        let node = self.cluster.get_random_node().await.ok_or(Error::NoNodes)?;
        let response = node.info(&[cmd]).await?;

        if let Some(v) = response.values().next() {
            if v.to_uppercase() == "OK" {
                return Ok(());
            } else if v.starts_with("FAIL:") {
                let result = v.split(':').nth(1).unwrap().parse::<u8>()?;
                return Err(Error::ServerError(ResultCode::from(result)));
            }
        }

        Err(Error::BadResponse(
            "unexpected sindex info command response".to_owned(),
        ))
    }
}
