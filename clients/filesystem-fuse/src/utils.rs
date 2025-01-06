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
use opendal::Operator;
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

pub(crate) async fn delete_dir(op: &Operator, dir_name: &str) {
    let childs = op.list(dir_name).await.expect("list dir failed");
    for child in childs {
        let child_name = dir_name.to_string() + child.name();
        if child.metadata().is_dir() {
            Box::pin(delete_dir(op, &child_name)).await;
        } else {
            op.delete(&child_name).await.expect("delete file failed");
        }
    }
    op.delete(dir_name).await.expect("delete dir failed");
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
