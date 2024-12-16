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
use crate::config::GravitinoConfig;
use crate::error::{ErrorCode, GravitinoError};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use urlencoding::encode;

#[derive(Debug, Deserialize)]
pub(crate) struct Fileset {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) fileset_type: String,
    comment: String,
    #[serde(rename = "storageLocation")]
    pub(crate) storage_location: String,
    properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct FilesetResponse {
    code: i32,
    fileset: Fileset,
}

#[derive(Debug, Deserialize)]
struct FileLocationResponse {
    code: i32,
    #[serde(rename = "fileLocation")]
    location: String,
}

pub(crate) struct GravitinoClient {
    gravitino_uri: String,
    metalake: String,

    http_client: Client,
}

impl GravitinoClient {
    pub fn new(config: &GravitinoConfig) -> Self {
        Self {
            gravitino_uri: config.gravitino_url.clone(),
            metalake: config.metalake.clone(),
            http_client: Client::new(),
        }
    }

    pub fn init(&self) {}

    pub fn do_post(&self, path: &str, data: &str) {
        println!("POST request to {} with data: {}", path, data);
    }

    pub fn do_get(&self, path: &str) {
        println!("GET request to {}", path);
    }

    pub fn request(&self, path: &str, data: &str) -> Result<(), GravitinoError> {
        todo!()
    }

    pub fn list_schema(&self) -> Result<(), GravitinoError> {
        todo!()
    }

    pub fn list_fileset(&self) -> Result<(), GravitinoError> {
        todo!()
    }

    fn get_fileset_url(&self, catalog_name: &str, schema_name: &str, fileset_name: &str) -> String {
        format!(
            "{}/api/metalakes/{}/catalogs/{}/schemas/{}/filesets/{}",
            self.gravitino_uri, self.metalake, catalog_name, schema_name, fileset_name
        )
    }

    async fn send_and_parse<T>(&self, url: &str) -> Result<T, GravitinoError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let http_resp = self.http_client.get(url).send().await.map_err(|e| {
            GravitinoError::RestError(format!("Failed to send request to {}", url), e)
        })?;

        let res = http_resp.json::<T>().await.map_err(|e| {
            GravitinoError::RestError(format!("Failed to parse response from {}", url), e)
        })?;

        Ok(res)
    }

    pub async fn get_fileset(
        &self,
        catalog_name: &str,
        schema_name: &str,
        fileset_name: &str,
    ) -> Result<Fileset, GravitinoError> {
        let url = self.get_fileset_url(catalog_name, schema_name, fileset_name);
        let res = self.send_and_parse::<FilesetResponse>(&url).await?;

        if res.code != 0 {
            return Err(GravitinoError::Error(
                ErrorCode::GravitinoClientError,
                "Failed to get fileset".to_string(),
            ));
        }
        Ok(res.fileset)
    }

    pub fn get_file_location_url(
        &self,
        catalog_name: &str,
        schema_name: &str,
        fileset_name: &str,
        path: &str,
    ) -> String {
        let encoded_path = encode(path);
        format!(
            "{}/api/metalakes/{}/catalogs/{}/schemas/{}/filesets/{}/location?sub_path={}",
            self.gravitino_uri, self.metalake, catalog_name, schema_name, fileset_name, path
        )
    }

    pub async fn get_file_location(
        &self,
        catalog_name: &str,
        schema_name: &str,
        fileset_name: &str,
        path: &str,
    ) -> Result<String, GravitinoError> {
        let url = self.get_file_location_url(catalog_name, schema_name, fileset_name, path);
        let res = self.send_and_parse::<FileLocationResponse>(&url).await?;

        if res.code != 0 {
            return Err(GravitinoError::Error(
                ErrorCode::GravitinoClientError,
                "Failed to get file location".to_string(),
            ));
        }
        Ok(res.location)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::mock;
    use tokio;

    #[tokio::test]
    async fn test_get_fileset_success() {
        tracing_subscriber::fmt::init();
        let fileset_response = r#"
        {
            "code": 0,
            "fileset": {
                "name": "example_fileset",
                "type": "example_type",
                "comment": "This is a test fileset",
                "storageLocation": "/example/path",
                "properties": {
                    "key1": "value1",
                    "key2": "value2"
                }
            }
        }"#;

        let mock_server_url = &mockito::server_url();

        let url = format!(
            "/api/metalakes/{}/catalogs/{}/schemas/{}/filesets/{}",
            "test", "catalog1", "schema1", "fileset1"
        );
        let _m = mock("GET", url.as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(fileset_response)
            .create();

        let config = GravitinoConfig {
            gravitino_url: mock_server_url.to_string(),
            metalake: "test".to_string(),
        };
        let client = GravitinoClient::new(&config);

        let result = client.get_fileset("catalog1", "schema1", "fileset1").await;

        match result {
            Ok(fileset) => {
                assert_eq!(fileset.name, "example_fileset");
                assert_eq!(fileset.fileset_type, "example_type");
                assert_eq!(fileset.storage_location, "/example/path");
                assert_eq!(fileset.properties.get("key1"), Some(&"value1".to_string()));
            }
            Err(e) => panic!("Expected Ok, but got Err: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_get_file_location_success() {
        tracing_subscriber::fmt::init();
        let file_location_response = r#"
        {
            "code": 0,
            "fileLocation": "/mybucket/a"
        }"#;

        let mock_server_url = &mockito::server_url();

        let url = format!(
            "/api/metalakes/{}/catalogs/{}/schemas/{}/filesets/{}/location?sub_path={}",
            "test", "catalog1", "schema1", "fileset1", "/example/path"
        );
        let _m = mock("GET", url.as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(file_location_response)
            .create();

        let config = GravitinoConfig {
            gravitino_url: mock_server_url.to_string(),
            metalake: "test".to_string(),
        };
        let client = GravitinoClient::new(&config);

        let result = client
            .get_file_location("catalog1", "schema1", "fileset1", "/example/path")
            .await;

        match result {
            Ok(location) => {
                assert_eq!(location, "/mybucket/a");
            }
            Err(e) => panic!("Expected Ok, but got Err: {:?}", e),
        }
    }
}
