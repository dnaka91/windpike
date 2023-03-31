use std::{iter::Peekable, str::Chars};

use super::{Host, ParseHostError, Result};

pub struct Parser<'a> {
    s: Peekable<Chars<'a>>,
    default_port: u16,
}

impl<'a> Parser<'a> {
    pub fn new(s: &'a str, default_port: u16) -> Self {
        Parser {
            s: s.chars().peekable(),
            default_port,
        }
    }

    pub fn read_hosts(&mut self) -> Result<Vec<Host>, ParseHostError> {
        let mut hosts = Vec::new();
        loop {
            let addr = self.read_addr_tuple()?;
            let (host, _tls_name, port) = match addr.len() {
                3 => (
                    addr[0].clone(),
                    Some(addr[1].clone()),
                    addr[2].parse().map_err(ParseHostError::PortNumber)?,
                ),
                2 => {
                    if let Ok(port) = addr[1].parse() {
                        (addr[0].clone(), None, port)
                    } else {
                        (addr[0].clone(), Some(addr[1].clone()), self.default_port)
                    }
                }
                1 => (addr[0].clone(), None, self.default_port),
                _ => return Err(ParseHostError::InvalidArgument),
            };
            // TODO: add TLS name
            hosts.push(Host::new(&host, port));

            match self.peek() {
                Some(&c) if c == ',' => self.next_char(),
                _ => break,
            };
        }

        Ok(hosts)
    }

    fn read_addr_tuple(&mut self) -> Result<Vec<String>, ParseHostError> {
        let mut parts = Vec::new();
        loop {
            let part = self.read_addr_part()?;
            parts.push(part);
            match self.peek() {
                Some(&c) if c == ':' => self.next_char(),
                _ => break,
            };
        }
        Ok(parts)
    }

    fn read_addr_part(&mut self) -> Result<String, ParseHostError> {
        let mut substr = String::new();
        loop {
            match self.peek() {
                Some(&c) if c != ':' && c != ',' => {
                    substr.push(c);
                    self.next_char();
                }
                _ => {
                    return if substr.is_empty() {
                        Err(ParseHostError::InvalidArgument)
                    } else {
                        Ok(substr)
                    }
                }
            }
        }
    }

    fn peek(&mut self) -> Option<&char> {
        self.s.peek()
    }

    fn next_char(&mut self) -> Option<char> {
        self.s.next()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{Host, Parser};

    #[test]
    fn read_addr_part() {
        assert_eq!(
            "foo".to_owned(),
            Parser::new("foo:bar", 3000).read_addr_part().unwrap()
        );
        assert_eq!(
            "foo".to_owned(),
            Parser::new("foo,bar", 3000).read_addr_part().unwrap()
        );
        assert_eq!(
            "foo".to_owned(),
            Parser::new("foo", 3000).read_addr_part().unwrap()
        );
        assert!(Parser::new("", 3000).read_addr_part().is_err());
        assert!(Parser::new(",", 3000).read_addr_part().is_err());
        assert!(Parser::new(":", 3000).read_addr_part().is_err());
    }

    #[test]
    fn read_addr_tuple() {
        assert_eq!(
            vec!["foo".to_owned()],
            Parser::new("foo", 3000).read_addr_tuple().unwrap()
        );
        assert_eq!(
            vec!["foo".to_owned(), "bar".to_owned()],
            Parser::new("foo:bar", 3000).read_addr_tuple().unwrap()
        );
        assert_eq!(
            vec!["foo".to_owned()],
            Parser::new("foo,", 3000).read_addr_tuple().unwrap()
        );
        assert!(Parser::new("", 3000).read_addr_tuple().is_err());
        assert!(Parser::new(",", 3000).read_addr_tuple().is_err());
        assert!(Parser::new(":", 3000).read_addr_tuple().is_err());
        assert!(Parser::new("foo:", 3000).read_addr_tuple().is_err());
    }

    #[test]
    fn read_hosts() {
        assert_eq!(
            vec![Host::new("foo", 3000)],
            Parser::new("foo", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new("foo", 3000)],
            Parser::new("foo:bar", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new("foo", 1234)],
            Parser::new("foo:1234", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new("foo", 1234)],
            Parser::new("foo:bar:1234", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new("foo", 1234), Host::new("bar", 1234)],
            Parser::new("foo:1234,bar:1234", 3000).read_hosts().unwrap()
        );
        assert!(Parser::new("", 3000).read_hosts().is_err());
        assert!(Parser::new(",", 3000).read_hosts().is_err());
        assert!(Parser::new("foo,", 3000).read_hosts().is_err());
        assert!(Parser::new(":", 3000).read_hosts().is_err());
        assert!(Parser::new("foo:", 3000).read_hosts().is_err());
        assert!(Parser::new("foo:bar:bar", 3000).read_hosts().is_err());
        assert!(Parser::new("foo:bar:1234:1234", 3000).read_hosts().is_err());
    }

    proptest! {
        #[test]
        fn read_random_hosts(name in any::<String>(), port in any::<u16>()) {
            Parser::new(&name,port).read_hosts().ok();
        }

        #[test]
        fn read_multiple_hosts(name in r#"\w+:\d{4}(,\w+:\d{4})+"#) {
            Parser::new(&name, 3000).read_hosts().unwrap();
        }
    }
}
