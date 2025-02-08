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
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "gvfs-fuse",
    version = "0.1",
    about = "A FUSE-based file system client"
)]
pub(crate) struct Arguments {
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    Mount {
        #[arg(help = "Mount point for the filesystem")]
        mount_point: String,

        #[arg(
            help = "The URI of the GVFS fileset, like gvfs://fileset/my_catalog/my_schema/my_fileset"
        )]
        fileset_location: String,

        #[arg(short, long, help = "Path to the configuration file")]
        config: Option<String>,

        #[arg(short, long, help = "Debug level", default_value_t = 0)]
        debug: u8,

        #[arg(short, long, default_value_t = false, help = "Run in foreground")]
        foreground: bool,
    },
    Umount {
        #[arg(help = "Mount point to umount")]
        mount_point: String,

        #[arg(short, long, help = "Force umount")]
        force: bool,
    },
}
