use std::net::SocketAddr;

use tonic::transport::Server;
use tonic_health::ServingStatus;

mod config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::Settings::from_env();
    let addr: SocketAddr = cfg.listen_addr.parse()?;

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", ServingStatus::Serving)
        .await;

    println!("qiita-data-plane listening on {addr}");

    Server::builder()
        .add_service(health_service)
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::config::Settings;

    #[test]
    fn config_defaults() {
        // Ensure Settings::from_env() produces expected defaults when no env vars are set.
        // Unset LISTEN_ADDR to guarantee default behaviour.
        std::env::remove_var("LISTEN_ADDR");
        let cfg = Settings::from_env();
        assert_eq!(cfg.listen_addr, "0.0.0.0:50051");
    }
}
