use std::{fmt, io, net::SocketAddr};

use super::{parser::Parser, ParseHostError, Result};

/// Host name/port of database server.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Host {
    /// Host name or IP address of database server.
    pub name: String,
    /// Port of database server.
    pub port: u16,
}

impl Host {
    /// Create a new host instance given a hostname/IP and a port number.
    #[must_use]
    pub fn new(name: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            port,
        }
    }

    /// Returns a string representation of the host's address.
    #[must_use]
    pub fn address(&self) -> String {
        format!("{}:{}", self.name, self.port)
    }

    /// Resolve the host into socket addresses.
    pub async fn to_socket_addrs(&self) -> io::Result<impl Iterator<Item = SocketAddr> + '_> {
        tokio::net::lookup_host((self.name.as_str(), self.port)).await
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.port)
    }
}

/// A trait for objects which can be converted to one or more `Host` values.
pub trait ToHosts {
    /// Converts this object into a list of `Host`s.
    ///
    /// # Errors
    ///
    /// Any errors encountered during conversion will be returned as an `Err`.
    fn to_hosts(&self) -> Result<Vec<Host>, ParseHostError>;
}

impl ToHosts for Vec<Host> {
    fn to_hosts(&self) -> Result<Vec<Host>, ParseHostError> {
        Ok(self.clone())
    }
}

impl ToHosts for String {
    fn to_hosts(&self) -> Result<Vec<Host>, ParseHostError> {
        self.as_str().to_hosts()
    }
}

impl<'a> ToHosts for &'a str {
    fn to_hosts(&self) -> Result<Vec<Host>, ParseHostError> {
        let mut parser = Parser::new(self, 3000);
        parser.read_hosts()
    }
}

#[cfg(test)]
mod tests {
    use super::{Host, ToHosts};

    #[test]
    fn to_hosts() {
        assert_eq!(
            vec![Host::new("foo", 3000)],
            String::from("foo").to_hosts().unwrap()
        );
        assert_eq!(vec![Host::new("foo", 3000)], "foo".to_hosts().unwrap());
        assert_eq!(vec![Host::new("foo", 1234)], "foo:1234".to_hosts().unwrap());
        assert_eq!(
            vec![Host::new("foo", 1234), Host::new("bar", 1234)],
            "foo:1234,bar:1234".to_hosts().unwrap()
        );
    }
}
