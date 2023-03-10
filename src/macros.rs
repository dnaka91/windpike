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

/// Constructs a new Value from one of the supported native data types.
#[macro_export]
macro_rules! as_val {
    ($val:expr) => {{
        $crate::Value::from($val)
    }};
}

/// Constructs a new `GeoJSON` Value from one of the supported native data types.
#[macro_export]
macro_rules! as_geo {
    ($val:expr) => {{
        $crate::Value::GeoJson($val.to_owned())
    }};
}

/// Constructs a new Blob Value from one of the supported native data types.
#[macro_export]
macro_rules! as_blob {
    ($val:expr) => {{
        $crate::Value::Blob($val)
    }};
}

/// Constructs a new List Value from a list of one or more native data types.
///
/// # Examples
///
/// Write a list value to a record bin.
///
/// ```rust
/// use aerospike::{as_bin, as_list, as_val, Client, ClientPolicy, Key, WritePolicy};
///
/// #[tokio::main]
/// async fn main() {
///     let client = Client::new(&ClientPolicy::default(), &"localhost:3000")
///         .await
///         .unwrap();
///
///     let key = Key::new("test", "test", "mykey").unwrap();
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
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push($crate::as_val!($v));
            )*
            $crate::Value::List(temp_vec)
        }
    };
}

/// Constructs a vector of Values from a list of one or more native data types.
#[macro_export]
macro_rules! as_values {
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push(as_val!($v));
            )*
            temp_vec
        }
    };
}

/// Constructs a Map Value from a list of key/value pairs.
///
/// # Examples
///
/// Write a map value to a record bin.
///
/// ```rust
/// use aerospike::{as_bin, Key, as_map, as_val, Client, ClientPolicy, WritePolicy};

/// #[tokio::main]
/// async fn main() {
///     let client = Client::new(&ClientPolicy::default(), &"localhost:3000")
///         .await
///         .unwrap();
///
///     let key = Key::new("test", "test", "mykey").unwrap();
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
    ( $( $k:expr => $v:expr),* ) => {
        {
            let mut temp_map = std::collections::HashMap::new();
            $(
                temp_map.insert(as_val!($k), as_val!($v));
            )*
            $crate::Value::HashMap(temp_map)
        }
    };
}
