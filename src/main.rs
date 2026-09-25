mod bridge;
mod config;
mod logging;
mod milky;
mod onebot;
mod state;
mod types;

use std::process::ExitCode;
use std::sync::Arc;

use tokio::signal;
use tokio::sync::{mpsc, watch};

use crate::bridge::Service;
use crate::config::Config;
use crate::milky::Client as MilkyClient;
use crate::onebot::Server;

const INBOUND_BUFFER: usize = 256;

#[tokio::main]
async fn main() -> ExitCode {
    let config_path = match parse_args() {
        Ok(p) => p,
        Err(code) => return code,
    };

    let cfg = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("load config failed: {e} (path: {config_path})");
            return ExitCode::from(1);
        }
    };

    logging::init(&cfg.bridge.log_level);
    tracing::info!(
        "========================= Milky OneBot Bridge v{} =========================",
        env!("CARGO_PKG_VERSION")
    );
    tracing::info!("Powered by Haruki Dev Team");

    tracing::info!(
        milky_ws = %cfg.milky.ws_endpoint,
        onebot_host = %cfg.onebot.host,
        onebot_port = cfg.onebot.port,
        message_format = %cfg.bridge.message_format,
        "starting milky onebot bridge",
    );

    let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_BUFFER);

    let upstream = match MilkyClient::new(&cfg.milky, inbound_tx) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!(err = %e, "create milky client failed");
            return ExitCode::from(1);
        }
    };

    let onebot_cfg = cfg.onebot.clone();
    let svc = Service::new(cfg, upstream);
    let server = Server::new(onebot_cfg, svc.clone());

    // Connects in the background and keeps reconnecting if the Milky server
    // is down or restarts; the OneBot side runs regardless.
    svc.start();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown_rx = shutdown_rx.clone();
        async move { server.run(shutdown_rx).await }
    });

    let service_task = tokio::spawn({
        let svc = svc.clone();
        let server = server.clone();
        let shutdown_rx = shutdown_rx.clone();
        async move { svc.run(server, inbound_rx, shutdown_rx).await }
    });

    wait_for_shutdown_signal().await;
    tracing::info!("shutdown signal received, draining tasks");
    let _ = shutdown_tx.send(true);

    if let Err(e) = service_task.await {
        tracing::warn!(err = %e, "service task join error");
    }
    match server_task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(err = %e, "onebot server exited with error"),
        Err(e) => tracing::warn!(err = %e, "server task join error"),
    }

    svc.shutdown().await;
    tracing::info!("bridge stopped");
    ExitCode::SUCCESS
}

fn parse_args() -> Result<String, ExitCode> {
    let mut path = "config.json".to_string();
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-config" | "--config" | "-c" => {
                path = match iter.next() {
                    Some(v) => v,
                    None => {
                        eprintln!("{arg} requires a value");
                        return Err(ExitCode::from(2));
                    }
                };
            }
            "-h" | "--help" => {
                println!("Usage: milky-ob11-bridge [--config <path>]");
                return Err(ExitCode::SUCCESS);
            }
            other if other.starts_with("--config=") => {
                path = other.trim_start_matches("--config=").to_string();
            }
            other => {
                eprintln!("unknown argument: {other}");
                return Err(ExitCode::from(2));
            }
        }
    }
    Ok(path)
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use signal::unix::{SignalKind, signal};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(err = %e, "install SIGTERM handler failed; using SIGINT only");
            let _ = signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        res = signal::ctrl_c() => {
            if let Err(e) = res {
                tracing::warn!(err = %e, "ctrl_c handler error");
            }
        }
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = signal::ctrl_c().await;
}
