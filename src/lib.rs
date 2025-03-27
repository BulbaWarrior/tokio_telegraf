//! Telegraf-rust provides a lightweight client library for writing metrics
//! to a InfluxDB Telegraf service.
//!
//! This library does not provide querying or other InfluxDB client-library
//! features. This is meant to be lightweight and simple for services
//! to report metrics.
//!
//! # How to use
//!
//! All usage will start by creating a socket connection via a [crate::Client]. This
//! supports multiple connection protocols - which one you use will be determined
//! by how your Telegraf `input.socket_listener` configuration is setup.
//!
//! Once a client is setup there are multiple different ways to write points.
//!
//! ## Define structs that represent metrics using the derive macro.
//!
//! ```no_run
//! use tokio_telegraf::*;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut client = Client::new("tcp://localhost:8094").await.unwrap();
//!
//!     #[derive(Metric)]
//!     struct MyMetric {
//!         field1: i32,
//!         #[telegraf(tag)]
//!         tag1: String,
//!     }
//!
//!     let point = MyMetric { field1: 1, tag1: "tag".to_owned() };
//!     client.write(&point).await;
//! }
//! ```
//!
//! As with any Telegraf point, tags are optional but at least one field
//! is required.
//!
//! By default the measurement name will be the same as the struct. You can
//! override this via derive attributes:
//!
//! ```
//! use tokio_telegraf::*;
//!
//! #[derive(Metric)]
//! #[measurement = "custom_name"]
//! struct MyMetric {
//!     field1: i32,
//! }
//! ```
//!
//! Timestamps are optional and can be set via the `timestamp` attribute:
//!
//! ```rust
//! use tokio_telegraf::*;
//!
//! #[derive(Metric)]
//! struct MyMetric {
//!     #[telegraf(timestamp)]
//!     ts: u64,
//!     field1: i32,
//! }
//! ```
//!
//! ## Use the [crate::point] macro to do ad-hoc metrics.
//!
//! ```no_run
//! use tokio_telegraf::*;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut client = Client::new("tcp://localhost:8094").await.unwrap();
//!
//!     let p = point!("measurement", ("tag1", "tag1Val"), ("field1", "field1Val"));
//!     client.write_point(&p).await;
//! }
//! ```
//!
//! The macro syntax is the following format:
//!
//! `(<measurement>, [(<tagName>, <tagVal>)], [(<fieldName>, <fieldVal>)]; <timestamp>)`
//!
//! Measurement name, tag set, and field set are comma separated. Tag and field
//! tuples are space separated. Timestamp is semicolon separated. The tag set and
//! timestamp are optional.
//!
//! ## Manual [crate::Point] initialization.
//!
//! ```no_run
//! use tokio_telegraf::{Client, Point};
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut c = Client::new("tcp://localhost:8094").await.unwrap();
//!
//!     let p = Point::new(
//!         String::from("measurement"),
//!         vec![
//!             (String::from("tag1"), String::from("tag1value"))
//!         ],
//!         vec![
//!             (String::from("field1"), Box::new(10)),
//!             (String::from("field2"), Box::new(20.5)),
//!             (String::from("field3"), Box::new("anything!"))
//!         ],
//!         Some(100),
//!     );
//!
//!     c.write_point(&p).await;
//! }
//! ```
//!
//! ### Field Data
//!
//! Any attribute that will be the value of a field must implement the `IntoFieldData` trait provided by this library.
//!
//! ```
//! use tokio_telegraf::FieldData;
//!
//! pub trait IntoFieldData {
//!     fn into_field_data(&self) -> FieldData;
//! }
//! ```
//!
//! Out of the box implementations are provided for many common data types, but manual implementation is possible for other data types.
//!
//! ### Timestamps
//!
//! Timestamps are an optional filed, if not present the Telegraf daemon will set the timestamp using the current time.
//! Timestamps are specified in nanosecond-precision Unix time, therefore `u64` must implement the `From<T>` trait for the field type, if the implementation is not already present:
//!
//! ```rust
//! use tokio_telegraf::*;
//!
//! #[derive(Copy, Clone)]
//! struct MyType {
//!     // ...
//! }
//!
//! impl From<MyType> for u64 {
//!     fn from(my_type: MyType) -> Self {
//!         todo!()
//!     }
//! }
//!
//! #[derive(Metric)]
//! struct MyMetric {
//!     #[telegraf(timestamp)]
//!     ts: MyType,
//!     field1: i32,
//! }
//!
//! ```
//!
//! More information about timestamps can be found [here](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/#timestamp).

pub mod macros;
pub mod protocol;

use std::{
    fmt, io,
    net::{Shutdown, SocketAddr},
};

#[cfg(target_family = "unix")]
use tokio::net::{UnixDatagram, UnixStream};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, UdpSocket},
};
use url::{Host, Url};

use protocol::*;
pub use protocol::{FieldData, IntoFieldData};
pub use tokio_telegraf_derive::*;

/// Common result type. Only meaningful response is
/// an error.
pub type TelegrafResult = Result<(), TelegrafError>;

/// Trait for writing custom types as a telegraf
/// [crate::Point].
///
/// For most use cases it is recommended to
/// derive this trait instead of manually
/// implementing it.
///
/// Used via [crate::Client::write].
///
/// # Examples
///
/// ```
/// use tokio_telegraf::*;
///
/// #[derive(Metric)]
/// #[measurement = "my_metric"]
/// struct MyMetric {
///     field1: i32,
///     #[telegraf(tag)]
///     tag1: String,
///     field2: f32,
///     #[telegraf(timestamp)]
///     ts: u64,
/// }
/// ```
pub trait Metric: Sync {
    /// Converts internal attributes
    /// to a Point format.
    fn to_point(&self) -> Point;
}

/// Error enum for library failures.
#[derive(Debug, thiserror::Error)]
pub enum TelegrafError {
    /// Error reading or writing I/O.
    #[error("I/O error {0}")]
    IoError(#[from] std::io::Error),
    /// Error with internal socket connection.
    #[error("connection error {0}")]
    ConnectionError(String),
    /// Error when a bad protocol is created.
    #[error("protocol error {0}")]
    BadProtocol(String),
}

/// A single influx metric. Handles conversion from Rust types
/// to influx lineprotocol syntax.
///
/// Telegraf protocol requires at least one field, whereas
/// tags are completely optional. Attempting to write a point
/// without any fields will return a [crate::TelegrafError].
///
/// Creation of points is made easier via the [crate::point] macro.
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    pub measurement: String,
    pub tags: Vec<Tag>,
    pub fields: Vec<Field>,
    pub timestamp: Option<Timestamp>,
}

/// Connection client used to handle socket connection management
/// and writing.
#[derive(Debug)]
pub struct Client {
    conn: Connector,
}

/// Different types of connections that the library supports.
#[derive(Debug)]
enum Connector {
    Tcp(TcpStream),
    Udp(UdpSocket),
    #[cfg(target_family = "unix")]
    Unix(UnixStream),
    #[cfg(target_family = "unix")]
    Unixgram(UnixDatagram),
}

impl Point {
    /// Creates a new Point that can be written using a [Client].
    pub fn new(
        measurement: String,
        tags: Vec<(String, String)>,
        fields: Vec<(String, Box<dyn IntoFieldData>)>,
        timestamp: Option<u64>,
    ) -> Self {
        let t = tags
            .into_iter()
            .map(|(n, v)| Tag { name: n, value: v })
            .collect();
        let f = fields
            .into_iter()
            .map(|(n, v)| Field {
                name: n,
                value: v.field_data(),
            })
            .collect();
        let ts = timestamp.map(|t| Timestamp { value: t });
        Self {
            measurement,
            tags: t,
            fields: f,
            timestamp: ts,
        }
    }

    fn to_lp(&self) -> LineProtocol {
        let tag_attrs: Vec<Attr> = self.tags.iter().cloned().map(Attr::Tag).collect();
        let field_attrs: Vec<Attr> = self.fields.iter().cloned().map(Attr::Field).collect();
        let timestamp_attr: Vec<Attr> = self
            .timestamp
            .iter()
            .cloned()
            .map(Attr::Timestamp)
            .collect();
        let tag_str = if tag_attrs.is_empty() {
            None
        } else {
            Some(format_attr(tag_attrs))
        };
        let field_str = format_attr(field_attrs);
        let timestamp_str = if timestamp_attr.is_empty() {
            None
        } else {
            Some(format_attr(timestamp_attr))
        };
        LineProtocol::new(self.measurement.clone(), tag_str, field_str, timestamp_str)
    }
}

impl Client {
    /// Creates a new Client. Determines socket protocol from
    /// provided URL.
    pub async fn new(conn_url: &str) -> Result<Self, TelegrafError> {
        let conn = Connector::new(conn_url).await?;
        Ok(Self { conn })
    }

    /// Writes the protocol representation of a point
    /// to the established connection.
    pub async fn write_point(&mut self, pt: &Point) -> TelegrafResult {
        if pt.fields.is_empty() {
            return Err(TelegrafError::BadProtocol(
                "points must have at least 1 field".to_owned(),
            ));
        }

        let lp = pt.to_lp();
        let bytes = lp.to_str().as_bytes();
        self.write_to_conn(bytes).await
    }

    /// Joins multiple points together and writes them in a batch. Useful
    /// if you want to write lots of points but not overwhelm local service or
    /// you want to ensure all points have the exact same timestamp.
    pub async fn write_points(&mut self, pts: &[Point]) -> TelegrafResult {
        if pts.iter().any(|p| p.fields.is_empty()) {
            return Err(TelegrafError::BadProtocol(
                "points must have at least 1 field".to_owned(),
            ));
        }

        let lp = pts
            .iter()
            .map(|p| p.to_lp().to_str().to_owned())
            .collect::<Vec<String>>()
            .join("");
        self.write_to_conn(lp.as_bytes()).await
    }

    /// Convenience wrapper around writing points for dynamically
    /// dispatched types that implement [crate::Metric]..
    pub async fn write_dyn(&mut self, metric: &dyn Metric) -> TelegrafResult {
        let pt = metric.to_point();
        self.write_point(&pt).await
    }

    /// Convenience wrapper around writing points for types
    /// that implement [crate::Metric].
    pub async fn write<M: Metric>(&mut self, metric: &M) -> TelegrafResult {
        let pt = metric.to_point();
        self.write_point(&pt).await
    }

    /// Closes and cleans up socket connection.
    pub async fn close(&mut self) -> io::Result<()> {
        self.conn.close().await
    }

    /// Writes byte array to internal outgoing socket.
    pub async fn write_to_conn(&mut self, data: &[u8]) -> TelegrafResult {
        self.conn.write(data).await.map(|_| Ok(()))?
    }
}

impl Connector {
    async fn close(&mut self) -> io::Result<()> {
        use Connector::*;
        match self {
            Tcp(c) => c.shutdown().await,
            #[cfg(target_family = "unix")]
            Unix(c) => c.shutdown().await,
            #[cfg(target_family = "unix")]
            Unixgram(c) => c.shutdown(Shutdown::Both),
            // Udp socket doesnt have a graceful close.
            Udp(_) => Ok(()),
        }
    }

    async fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        let r = match self {
            Self::Tcp(c) => c.write(buf).await,
            Self::Udp(c) => c.send(buf).await,
            #[cfg(target_family = "unix")]
            Self::Unix(c) => c.write(buf).await,
            #[cfg(target_family = "unix")]
            Self::Unixgram(c) => c.send(buf).await,
        };
        r.map(|_| Ok(()))?
    }

    async fn new(url: &str) -> Result<Self, TelegrafError> {
        fn io_result<T>(opt: Option<T>, message: &str) -> io::Result<T> {
            opt.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, message))
        }
        match Url::parse(url) {
            Ok(u) => {
                let scheme = u.scheme();
                match scheme {
                    "tcp" => {
                        let conn = {
                            let host = io_result(u.host(), "No host name in the URL")?;
                            let port = io_result(u.port(), "No port number in the URL")?;
                            match host {
                                url::Host::Domain(domain) => {
                                    TcpStream::connect((domain, port)).await?
                                }
                                url::Host::Ipv4(ipv4_addr) => {
                                    TcpStream::connect((ipv4_addr, port)).await?
                                }
                                url::Host::Ipv6(ipv6_addr) => {
                                    TcpStream::connect((ipv6_addr, port)).await?
                                }
                            }
                        };
                        Ok(Connector::Tcp(conn))
                    }
                    "udp" => {
                        let host = io_result(u.host(), "No host name in the URL")?;
                        let port = io_result(u.port(), "No port number in the URL")?;
                        let conn =
                            UdpSocket::bind(&[SocketAddr::from(([0, 0, 0, 0], 0))][..]).await?;
                        match host {
                            url::Host::Domain(domain) => conn.connect((domain, port)).await?,
                            url::Host::Ipv4(ipv4_addr) => conn.connect((ipv4_addr, port)).await?,
                            url::Host::Ipv6(ipv6_addr) => conn.connect((ipv6_addr, port)).await?,
                        }
                        // conn.set_nonblocking(true)?;
                        Ok(Connector::Udp(conn))
                    }
                    #[cfg(target_family = "unix")]
                    "unix" => {
                        let path = u.path();
                        let conn = UnixStream::connect(path).await?;
                        Ok(Connector::Unix(conn))
                    }
                    #[cfg(target_family = "unix")]
                    "unixgram" => {
                        let path = u.path();
                        let conn = UnixDatagram::unbound()?;
                        conn.connect(path)?;
                        // conn.set_nonblocking(true)?;
                        Ok(Connector::Unixgram(conn))
                    }
                    _ => Err(TelegrafError::BadProtocol(format!(
                        "unknown connection protocol {}",
                        scheme
                    ))),
                }
            }
            Err(_) => Err(TelegrafError::BadProtocol(format!(
                "invalid connection URL {}",
                url
            ))),
        }
    }
}

trait TelegrafUnwrap<T> {
    fn t_unwrap(self, msg: &str) -> Result<T, TelegrafError>;
}

impl<T> TelegrafUnwrap<T> for Option<T> {
    fn t_unwrap(self, msg: &str) -> Result<T, TelegrafError> {
        self.ok_or_else(|| TelegrafError::ConnectionError(msg.to_owned()))
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_lp().to_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_create_point_lp_ts_no_tags() {
        let p = Point::new(
            String::from("Foo"),
            vec![],
            vec![
                ("f1".to_owned(), Box::new(10)),
                ("f2".to_owned(), Box::new(10.3)),
            ],
            Some(10),
        );

        let lp = p.to_lp();
        assert_eq!(lp.to_str(), "Foo f1=10i,f2=10.3 10\n");
    }

    #[test]
    fn can_create_point_lp_ts() {
        let p = Point::new(
            String::from("Foo"),
            vec![("t1".to_owned(), "v".to_owned())],
            vec![
                ("f1".to_owned(), Box::new(10)),
                ("f2".to_owned(), Box::new(10.3)),
                ("f3".to_owned(), Box::new("b")),
            ],
            Some(10),
        );

        let lp = p.to_lp();
        assert_eq!(lp.to_str(), "Foo,t1=v f1=10i,f2=10.3,f3=\"b\" 10\n");
    }

    #[test]
    fn can_create_point_lp() {
        let p = Point::new(
            String::from("Foo"),
            vec![("t1".to_owned(), "v".to_owned())],
            vec![
                ("f1".to_owned(), Box::new(10)),
                ("f2".to_owned(), Box::new(10.3)),
                ("f3".to_owned(), Box::new("b")),
            ],
            None,
        );

        let lp = p.to_lp();
        assert_eq!(lp.to_str(), "Foo,t1=v f1=10i,f2=10.3,f3=\"b\"\n");
    }

    #[test]
    fn can_create_point_lp_no_tags() {
        let p = Point::new(
            String::from("Foo"),
            vec![],
            vec![
                ("f1".to_owned(), Box::new(10)),
                ("f2".to_owned(), Box::new(10.3)),
            ],
            None,
        );

        let lp = p.to_lp();
        assert_eq!(lp.to_str(), "Foo f1=10i,f2=10.3\n");
    }
}
