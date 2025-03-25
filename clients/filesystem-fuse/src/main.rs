/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
mod command_args;

use crate::command_args::Commands;
use clap::Parser;
use daemonize::Daemonize;
use gvfs_fuse::config::AppConfig;
use gvfs_fuse::{gvfs_mount, gvfs_unmount, LOG_FILE_NAME, PID_FILE_NAME};
use std::fs::{create_dir, OpenOptions};
use std::path::Path;
use std::process::Command;
use std::{env, io};
use tokio::runtime::Runtime;
use tokio::signal;
use tokio::signal::unix::{signal, SignalKind};
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::Layered;
use tracing_subscriber::{filter, fmt, prelude::*, reload, EnvFilter, Registry};

fn init_dirs(config: &mut AppConfig, mount_point: &str) -> io::Result<()> {
    let data_dir_name = Path::new(&config.fuse.data_dir).to_path_buf();
    if !data_dir_name.exists() {
        create_dir(&data_dir_name)?
    };
    let data_dir_name = data_dir_name.canonicalize()?;
    config.fuse.data_dir = data_dir_name.to_string_lossy().to_string();

    let mount_point_name = data_dir_name.join(mount_point);
    if !mount_point_name.exists() {
        create_dir(&mount_point_name)?
    };

    let log_dir_name = data_dir_name.join(&config.fuse.log_dir);
    config.fuse.log_dir = log_dir_name.to_string_lossy().to_string();
    if !log_dir_name.exists() {
        create_dir(&log_dir_name)?
    };

    Ok(())
}

fn make_daemon(config: &AppConfig) -> io::Result<()> {
    let data_dir_name = Path::new(&config.fuse.data_dir);
    let log_dir_name = data_dir_name.join(&config.fuse.log_dir);
    let log_file_name = log_dir_name.join(LOG_FILE_NAME);
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_name)
        .unwrap();
    let log_err_file = OpenOptions::new().append(true).open(log_file_name).unwrap();

    let pid_file_name = data_dir_name.join(PID_FILE_NAME);

    let daemonize = Daemonize::new()
        .pid_file(pid_file_name)
        .chown_pid_file(true)
        .working_directory(data_dir_name)
        .stdout(log_file)
        .stderr(log_err_file);

    match daemonize.start() {
        Ok(_) => info!("Gvfs-fuse Daemon started successfully"),
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Gvfs-fuse Daemon failed to start: {:?}", e),
            ))
        }
    }
    Ok(())
}

fn mount_fuse(config: AppConfig, mount_point: String, target: String) -> io::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        let handle = tokio::spawn(async move {
            let result = gvfs_mount(&mount_point, &target, &config).await;
            if let Err(e) = result {
                error!("Failed to mount gvfs: {:?}", e);
                return Err(io::Error::from(io::ErrorKind::InvalidInput));
            }
            Ok(())
        });

        let mut term_signal = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = handle => {}
            _ = signal::ctrl_c() => {
                    info!("Received Ctrl+C, unmounting gvfs...")
                }
            _ = term_signal.recv()=> {
                    info!("Received SIGTERM, unmounting gvfs...")
                }
        }

        let _ = gvfs_unmount().await;
        Ok(())
    })
}

#[cfg(target_os = "macos")]
fn do_umount(mp: &str, force: bool) -> std::io::Result<()> {
    let cmd_result = if force {
        Command::new("umount").arg("-f").arg(mp).output()
    } else {
        Command::new("umount").arg(mp).output()
    };

    handle_command_result(cmd_result)
}

#[cfg(target_os = "linux")]
fn do_umount(mp: &str, force: bool) -> std::io::Result<()> {
    let cmd_result =
        if Path::new("/bin/fusermount").exists() || Path::new("/usr/bin/fusermount").exists() {
            if force {
                Command::new("fusermount").arg("-uz").arg(mp).output()
            } else {
                Command::new("fusermount").arg("-u").arg(mp).output()
            }
        } else if force {
            Command::new("umount").arg("-l").arg(mp).output()
        } else {
            Command::new("umount").arg(mp).output()
        };

    handle_command_result(cmd_result)
}

fn handle_command_result(cmd_result: io::Result<std::process::Output>) -> io::Result<()> {
    match cmd_result {
        Ok(output) => {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                Err(io::Error::new(io::ErrorKind::Other, stderr.to_string()))
            } else {
                Ok(())
            }
        }
        Err(e) => Err(e),
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn do_umount(_mp: &str, _force: bool) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("OS {} is not supported", env::consts::OS),
    ))
}

/// init tracing subscriber with directives, and returns reload handle to allow us to modify filter dynamically.
fn init_tracing_subscriber(
    directives: &str,
) -> reload::Handle<EnvFilter, Layered<Layer<Registry>, Registry>> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse(directives)
        .unwrap();

    let (filter, reload_handle) = reload::Layer::new(env_filter);

    // Initialize the subscriber
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();

    reload_handle
}

fn main() -> Result<(), i32> {
    let directives = env::var("RUST_LOG").unwrap_or("".to_string());
    let args = command_args::Arguments::parse();
    let reload_handle = init_tracing_subscriber(directives.as_str());
    match args.command {
        Commands::Mount {
            mount_point,
            fileset_location,
            config,
            debug: debug_level,
            foreground,
        } => {
            let app_config = AppConfig::from_file(config);
            if let Err(e) = &app_config {
                error!("Failed to load config: {:?}", e);
                return Err(-1);
            };
            let mut app_config = app_config.unwrap();

            let mount_point = {
                let path = Path::new(&mount_point).canonicalize();
                if let Err(e) = path {
                    error!("Failed to resolve mount point: {:?}", e);
                    return Err(-1);
                };
                let path = path.unwrap();
                path.to_string_lossy().to_string()
            };

            // if debug > 0, it means that we needs fuse_debug.
            app_config.fuse.fuse_debug = debug_level > 0 || app_config.fuse.fuse_debug;
            match debug_level {
                0 => {
                    // Use the value in RUST_LOG, no need to modify filter
                }
                1 => {
                    // debug feature of gvfs_fuse is enabled.
                    reload_handle
                        .modify(|filter| {
                            *filter = filter::EnvFilter::builder()
                                .parse([directives.as_str(), "gvfs_fuse=debug"].join(",").as_str())
                                .unwrap();
                        })
                        .unwrap();
                }
                _ => {
                    error!("Unsupported debug level: {}", debug_level);
                    return Err(-1);
                }
            }

            let result = init_dirs(&mut app_config, &mount_point);
            if let Err(e) = result {
                error!("Failed to initialize working directories: {:?}", e);
                return Err(-1);
            }

            let result = if foreground {
                mount_fuse(app_config, mount_point, fileset_location)
            } else {
                let result = make_daemon(&app_config);
                info!("Making daemon");
                if let Err(e) = result {
                    error!("Failed to daemonize: {:?}", e);
                    return Err(-1);
                };
                mount_fuse(app_config, mount_point, fileset_location)
            };

            if let Err(e) = result {
                error!("Failed to mount gvfs: {:?}", e.to_string());
                return Err(-1);
            };
            Ok(())
        }
        Commands::Umount { mount_point, force } => {
            let result = do_umount(&mount_point, force);
            if let Err(e) = result {
                error!("Failed to unmount gvfs: {:?}", e.to_string());
                return Err(-1);
            };
            Ok(())
        }
    }
}
