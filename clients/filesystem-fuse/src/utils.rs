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
use crate::error::ErrorCode::InvalidConfig;
use crate::error::GvfsError;
use reqwest::Url;
use std::path::PathBuf;

pub type GvfsResult<T> = Result<T, GvfsError>;

pub(crate) fn parse_location(location: &str) -> GvfsResult<Url> {
    let parsed_url = Url::parse(location);
    if let Err(e) = parsed_url {
        return Err(InvalidConfig.to_error(format!("Invalid fileset location: {}", e)));
    }
    Ok(parsed_url.unwrap())
}

pub(crate) fn extract_root_path(location: &str) -> GvfsResult<PathBuf> {
    let url = parse_location(location)?;
    Ok(PathBuf::from(url.path()))
}

#[cfg(test)]
mod tests {
    use crate::utils::extract_root_path;
    use std::path::PathBuf;

    #[test]
    fn test_extract_root_path() {
        let location = "s3://bucket/path/to/file";
        let result = extract_root_path(location);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("/path/to/file"));
    }
}
