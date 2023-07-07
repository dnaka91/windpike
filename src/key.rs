use std::borrow::Cow;

use ripemd::{Digest, Ripemd160};

use crate::{commands::ParticleType, msgpack, value::ParticleError};

macro_rules! from {
    ($to:ty, $variant:ident, $($from:ty),+) => {
        $(impl From<$from> for $to {
            fn from(value: $from) -> Self {
                Self::$variant(value.into())
            }
        })+
    };
}

/// Unique record identifier. Records can be identified using a specified namespace, an optional
/// set name and a user defined key which must be unique within a set. Records can also be
/// identified by namespace/digest, which is the combination used on the server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Key {
    /// Namespace.
    pub namespace: Cow<'static, str>,
    /// Set name.
    pub set_name: Cow<'static, str>,
    /// Original user key.
    pub user_key: Option<UserKey>,
    /// Unique server hash value generated from set name and user key.
    pub(crate) digest: [u8; 20],
}

impl Key {
    /// Construct a new key given a namespace, a set name and a user key value.
    ///
    /// # Panics
    ///
    /// Only integers, strings and blobs (`Vec<u8>`) can be used as user keys. The constructor will
    /// panic if any other value type is passed.
    pub fn new<N, S, K>(namespace: N, set_name: S, key: K) -> Self
    where
        N: Into<Cow<'static, str>>,
        S: Into<Cow<'static, str>>,
        K: Into<UserKey>,
    {
        let set_name = set_name.into();
        let user_key = key.into();
        let digest = Self::compute_digest(&set_name, &user_key);

        Self {
            namespace: namespace.into(),
            set_name,
            digest,
            user_key: Some(user_key),
        }
    }

    #[must_use]
    pub fn digest(&self) -> [u8; 20] {
        self.digest
    }

    fn compute_digest(set_name: &str, user_key: &UserKey) -> [u8; 20] {
        let mut hash = Ripemd160::new();
        hash.update(set_name.as_bytes());
        hash.update([user_key.particle_type() as u8]);
        user_key.write_key_bytes(&mut hash);

        hash.finalize().into()
    }
}

/// The user key, which is a subset of the [`Value`](crate::Value) type, as only a few of its
/// variants are allowed to be used in Aerospike keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UserKey {
    /// 64-bit signed integer.
    Int(i64),
    /// String value.
    String(Cow<'static, str>),
    /// Byte array value.
    Blob(Cow<'static, [u8]>),
}

impl UserKey {
    /// If this value is a 64-bit signed integer, return the associated `i64`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::UserKey;
    /// let v = UserKey::from(10_i64);
    ///
    /// assert_eq!(Some(10), v.as_i64());
    /// assert_eq!(None, v.as_str());
    /// ```
    #[inline]
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    /// If this value is a string, return the associated `&str`. Return `None` oterwhise.
    ///
    /// ```
    /// # use windpike::UserKey;
    /// let v = UserKey::from("value");
    ///
    /// assert_eq!(Some("value"), v.as_str());
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    /// If this value is a blob, return the associated `&[u8]`. Return `None` oterwhise.
    ///
    /// ```
    /// # use windpike::UserKey;
    /// let v = UserKey::from(&[1, 2, 3][..]);
    ///
    /// assert_eq!(Some(&[1, 2, 3][..]), v.as_bytes());
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(value) => Some(value),
            _ => None,
        }
    }

    /// If this value is a string, return the associated `String`. Return `None` oterwhise. In
    /// contrast to [`Self::as_str`], this method consumes the value to return the owned string.
    ///
    /// ```
    /// # use std::borrow::Cow;
    /// # use windpike::UserKey;
    /// let v = UserKey::from("value");
    ///
    /// assert_eq!(Some(Cow::Owned(String::from("value"))), v.into_string());
    /// ```
    #[inline]
    #[must_use]
    pub fn into_string(self) -> Option<Cow<'static, str>> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    /// If this value is a blob, return the associated `Vec<u8>`. Return `None` oterwhise. In
    /// contrast to [`Self::as_bytes`], this method consumes the value to return the owned vector.
    ///
    /// ```
    /// # use std::borrow::Cow;
    /// # use windpike::UserKey;
    /// let v = UserKey::from(&[1, 2, 3][..]);
    ///
    /// assert_eq!(Some(Cow::Owned(vec![1, 2, 3])), v.into_bytes());
    /// ```
    #[inline]
    #[must_use]
    pub fn into_bytes(self) -> Option<Cow<'static, [u8]>> {
        match self {
            Self::Blob(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn particle_type(&self) -> ParticleType {
        match self {
            UserKey::Int(_) => ParticleType::Integer,
            UserKey::String(_) => ParticleType::String,
            UserKey::Blob(_) => ParticleType::Blob,
        }
    }

    fn write_key_bytes(&self, hasher: &mut impl Digest) {
        match self {
            UserKey::Int(i) => hasher.update(i.to_be_bytes()),
            UserKey::String(s) => hasher.update(s.as_bytes()),
            UserKey::Blob(b) => hasher.update(b),
        }
    }

    pub(crate) fn estimate_size(&self) -> usize {
        match self {
            UserKey::Int(_) => 8,
            UserKey::String(s) => s.len(),
            UserKey::Blob(b) => b.len(),
        }
    }

    pub(crate) fn write_to(&self, w: &mut impl msgpack::Write) -> usize {
        match self {
            UserKey::Int(i) => w.write_i64(*i),
            UserKey::String(s) => w.write_str(s),
            UserKey::Blob(b) => w.write_bytes(b),
        }
    }

    pub(crate) fn read_from(
        r: &mut impl msgpack::Read,
        particle_type: u8,
        length: usize,
    ) -> Result<Self, ParticleError> {
        Ok(match ParticleType::try_from(particle_type)? {
            ParticleType::Integer => Self::Int(r.read_i64()),
            ParticleType::String => Self::String(r.read_str(length)?.into()),
            ParticleType::Blob => Self::Blob(r.read_bytes(length).into()),
            _ => return Err(ParticleError::Unsupported(particle_type)),
        })
    }
}

from!(UserKey, Int, i8, i16, i32, i64, u8, u16, u32);
from!(UserKey, String, &'static str, String, Cow<'static, str>);
from!(UserKey, Blob, &'static [u8], Vec<u8>, Cow<'static, [u8]>);

#[cfg(test)]
mod tests {
    use std::str;

    use crate::Key;

    macro_rules! digest {
        ($x:expr) => {
            Key::new("namespace", "set", $x)
                .digest
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>()
        };
    }
    macro_rules! str_repeat {
        ($c:expr, $n:expr) => {
            str::from_utf8(&[$c as u8; $n]).unwrap()
        };
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn int_keys() {
        assert_eq!(digest!(0), "93d943aae37b017ad7e011b0c1d2e2143c2fb37d");
        assert_eq!(digest!(-1), "22116d253745e29fc63fdf760b6e26f7e197e01d");

        assert_eq!(digest!(1i8), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1u8), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1i16), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1u16), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1i32), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1u32), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1i64), "82d7213b469812947c109a6d341e3b5b1dedec1f");

        assert_eq!(
            digest!(i64::MIN),
            "7185c2a47fb02c996daed26b4e01b83240aee9d4"
        );
        assert_eq!(
            digest!(i64::MAX),
            "1698328974afa62c8e069860c1516f780d63dbb8"
        );
        assert_eq!(
            digest!(i32::MIN),
            "d635a867b755f8f54cdc6275e6fb437df82a728c"
        );
        assert_eq!(
            digest!(i32::MAX),
            "fa8c47b8b898af1bbcb20af0d729ca68359a2645"
        );
        assert_eq!(
            digest!(i16::MIN),
            "7f41e9dd1f3fe3694be0430e04c8bfc7d51ec2af"
        );
        assert_eq!(
            digest!(i16::MAX),
            "309fc9c2619c4f65ff7f4cd82085c3ee7a31fc7c"
        );
        assert_eq!(digest!(i8::MIN), "93191e549f8f3548d7e2cfc958ddc8c65bcbe4c6");
        assert_eq!(digest!(i8::MAX), "a58f7d98bf60e10fe369c82030b1c9dee053def9");

        assert_eq!(
            digest!(u32::MAX),
            "2cdf52bf5641027042b9cf9a499e509a58b330e2"
        );
        assert_eq!(
            digest!(u16::MAX),
            "3f0dd44352749a9fd5b7ec44213441ef54c46d57"
        );
        assert_eq!(digest!(u8::MAX), "5a7dd3ea237c30c8735b051524e66fd401a10f6a");
    }

    #[test]
    fn string_keys() {
        assert_eq!(digest!(""), "2819b1ff6e346a43b4f5f6b77a88bc3eaac22a83");
        assert_eq!(
            digest!(str_repeat!('s', 1)),
            "607cddba7cd111745ef0a3d783d57f0e83c8f311"
        );
        assert_eq!(
            digest!(str_repeat!('a', 10)),
            "5979fb32a80da070ff356f7695455592272e36c2"
        );
        assert_eq!(
            digest!(str_repeat!('m', 100)),
            "f00ad7dbcb4bd8122d9681bca49b8c2ffd4beeed"
        );
        assert_eq!(
            digest!(str_repeat!('t', 1000)),
            "07ac412d4c33b8628ab147b8db244ce44ae527f8"
        );
        assert_eq!(
            digest!(str_repeat!('-', 10000)),
            "b42e64afbfccb05912a609179228d9249ea1c1a0"
        );
        assert_eq!(
            digest!(str_repeat!('+', 100_000)),
            "0a3e888c20bb8958537ddd4ba835e4070bd51740"
        );

        assert_eq!(digest!("haha"), "36eb02a807dbade8cd784e7800d76308b4e89212");
        assert_eq!(
            digest!("haha".to_owned()),
            "36eb02a807dbade8cd784e7800d76308b4e89212"
        );
    }

    #[test]
    fn blob_keys() {
        assert_eq!(
            digest!(vec![0u8; 0]),
            "327e2877b8815c7aeede0d5a8620d4ef8df4a4b4"
        );
        assert_eq!(
            digest!(vec![b's'; 1]),
            "ca2d96dc9a184d15a7fa2927565e844e9254e001"
        );
        assert_eq!(
            digest!(vec![b'a'; 10]),
            "d10982327b2b04c7360579f252e164a75f83cd99"
        );
        assert_eq!(
            digest!(vec![b'm'; 100]),
            "475786aa4ee664532a7d1ea69cb02e4695fcdeed"
        );
        assert_eq!(
            digest!(vec![b't'; 1000]),
            "5a32b507518a49bf47fdaa3deca53803f5b2e8c3"
        );
        assert_eq!(
            digest!(vec![b'-'; 10000]),
            "ed65c63f7a1f8c6697eb3894b6409a95461fd982"
        );
        assert_eq!(
            digest!(vec![b'+'; 100_000]),
            "fe19770c371774ba1a1532438d4851b8a773a9e6"
        );
    }
}
