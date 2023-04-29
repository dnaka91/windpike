/// Construct a new bin from a name and an optional value (defaults to the empty value `nil`).
#[macro_export]
macro_rules! as_bin {
    ($bin_name:expr, None) => {{
        $crate::Bin::new($bin_name, $crate::Value::Nil)
    }};
    ($bin_name:expr, $val:expr) => {{
        $crate::Bin::new($bin_name, $crate::Value::from($val))
    }};
}

/// Constructs a new List Value from a list of one or more native data types.
///
/// # Examples
///
/// Write a list value to a record bin.
///
/// ```rust
/// use windpike::{
///     as_bin, as_list,
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
///     let list = as_list!("a", "b", "c");
///     let bin = as_bin!("list", list);
///     client
///         .put(&WritePolicy::default(), &key, &vec![bin])
///         .await
///         .unwrap();
/// }
/// ```
#[macro_export]
macro_rules! as_list {
    ($($v:expr),*) => {{
        $crate::Value::List(
            vec![$($crate::Value::from($v),)*]
        )
    }};
}

/// Constructs a vector of Values from a list of one or more native data types.
#[macro_export]
macro_rules! as_values {
    ($($v:expr),*) => {{
        vec![$($crate::Value::from($v),)*]
    }};
}

/// Constructs a Map Value from a list of key/value pairs.
///
/// # Examples
///
/// Write a map value to a record bin.
///
/// ```rust
/// use windpike::{as_bin, Key, as_map, Client, policy::ClientPolicy, policy::WritePolicy};

/// #[tokio::main]
/// async fn main() {
///     let client = Client::new(&ClientPolicy::default(), "localhost:3000")
///         .await
///         .unwrap();
///
///     let key = Key::new("test", "test", "mykey");
///     let map = as_map!("a" => 1, "b" => 2);
///     let bin = as_bin!("map", map);
///     client
///         .put(&WritePolicy::default(), &key, &vec![bin])
///         .await
///         .unwrap();
/// }
/// ```
#[macro_export]
macro_rules! as_map {
    ($($k:expr => $v:expr),*) => {{
        $crate::Value::HashMap(
            std::collections::HashMap::from([
                $(($crate::MapKey::from($k), $crate::Value::from($v)),)*
            ])
        )
    }};
}
