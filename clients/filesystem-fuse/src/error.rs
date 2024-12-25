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
use fuse3::Errno;

#[derive(Debug)]
pub enum ErrorCode {
    UnSupportedFilesystem,
    GravitinoClientError,
}

impl ErrorCode {
    pub fn to_string(&self) -> String {
        match self {
            ErrorCode::UnSupportedFilesystem => "The filesystem is not supported".to_string(),
            _ => "".to_string(),
        }
    }
    pub fn to_error(self, message: impl Into<String>) -> GvfsError {
        GvfsError::Error(self, message.into())
    }
}

#[derive(Debug)]
pub enum GvfsError {
    RestError(String, reqwest::Error),
    Error(ErrorCode, String),
    Errno(Errno),
    IOError(std::io::Error),
}
impl From<reqwest::Error> for GvfsError {
    fn from(err: reqwest::Error) -> Self {
        GvfsError::RestError("Http request failed:".to_owned() + &err.to_string(), err)
    }
}

impl From<Errno> for GvfsError {
    fn from(errno: Errno) -> Self {
        GvfsError::Errno(errno)
    }
}

impl From<std::io::Error> for GvfsError {
    fn from(err: std::io::Error) -> Self {
        GvfsError::IOError(err)
    }
}