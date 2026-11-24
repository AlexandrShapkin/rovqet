use std::net::TcpListener;

pub fn reserve() -> TcpListener {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve endpoint");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    listener
}

/// Keep the original descriptor bound even while a voter is shut down. A restart
/// uses another descriptor for the same socket, never a release-and-rebind.
pub fn listener(reserved: &TcpListener) -> tokio::net::TcpListener {
    tokio::net::TcpListener::from_std(reserved.try_clone().expect("clone reservation"))
        .expect("register listener")
}
