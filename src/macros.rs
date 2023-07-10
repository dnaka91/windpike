/// Constructs a [`Vec`]<[`Value`](crate::Value)> of from a list of native data types.
#[macro_export]
macro_rules! values {
    ($($v:expr),* $(,)?) => {{
        vec![$($crate::Value::from($v),)*]
    }};
}

/// Constructs a [`Value::List`](crate::Value::List) from a list of native data types.
///
/// # Examples
///
/// Write a list value to a record.
///
/// ```rust
/// use windpike::{
///     policies::{ClientPolicy, WritePolicy},
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
///     let bin = Bin::new("list", windpike::list!("a", 1, true));
///
///     client
///         .put(&WritePolicy::default(), &key, &[bin])
///         .await
///         .unwrap();
/// }
/// ```
#[macro_export]
macro_rules! list {
    ($($v:expr),* $(,)?) => {{
        $crate::Value::List(
            $crate::values!($($v,)*)
        )
    }};
}

/// Constructs a [`MapKey`](crate::MapKey) from a list of key/value pairs.
///
/// # Examples
///
/// Write a map value to a record.
///
/// ```rust
/// use windpike::{Bin, Key, Client, policies::ClientPolicy, policies::WritePolicy};
///
/// #[tokio::main]
/// async fn main() {
///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
///         .await
///         .unwrap();
///
///     let key = Key::new("test", "test", "mykey");
///     let bin = Bin::new("map", windpike::map!("a" => true, 2 => 10.0));
///
///     client
///         .put(&WritePolicy::default(), &key, &[bin])
///         .await
///         .unwrap();
/// }
/// ```
#[macro_export]
macro_rules! map {
    ($($k:expr => $v:expr),* $(,)?) => {{
        $crate::Value::HashMap(
            [$(($crate::MapKey::from($k), $crate::Value::from($v)),)*].into()
        )
    }};
}
