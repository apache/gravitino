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
use crate::error::{ErrorCode, GvfsError};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
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
    code: u32,
    fileset: Fileset,
}

#[derive(Debug, Deserialize)]
struct FileLocationResponse {
    code: u32,
    #[serde(rename = "fileLocation")]
    location: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Catalog {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) catalog_type: String,
    provider: String,
    comment: String,
    pub(crate) properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct CatalogResponse {
    code: u32,
    catalog: Catalog,
}

pub(crate) struct GravitinoClient {
    gravitino_uri: String,
    metalake: String,

    client: Client,
}

impl GravitinoClient {
    pub fn new(config: &GravitinoConfig) -> Self {
        Self {
            gravitino_uri: config.uri.clone(),
            metalake: config.metalake.clone(),
            client: Client::new(),
        }
    }

    pub fn init(&self) {}

    pub fn do_post(&self, _path: &str, _data: &str) {
        todo!()
    }

    pub fn request(&self, _path: &str, _data: &str) -> Result<(), GvfsError> {
        todo!()
    }

    pub fn list_schema(&self) -> Result<(), GvfsError> {
        todo!()
    }

    pub fn list_fileset(&self) -> Result<(), GvfsError> {
        todo!()
    }

    fn get_fileset_url(&self, catalog_name: &str, schema_name: &str, fileset_name: &str) -> String {
        format!(
            "{}/api/metalakes/{}/catalogs/{}/schemas/{}/filesets/{}",
            self.gravitino_uri, self.metalake, catalog_name, schema_name, fileset_name
        )
    }

    async fn do_get<T>(&self, url: &str) -> Result<T, GvfsError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let http_resp =
            self.client.get(url).send().await.map_err(|e| {
                GvfsError::RestError(format!("Failed to send request to {}", url), e)
            })?;

        let res = http_resp.json::<T>().await.map_err(|e| {
            GvfsError::RestError(format!("Failed to parse response from {}", url), e)
        })?;

        Ok(res)
    }

    pub async fn get_catalog_url(&self, catalog_name: &str) -> String {
        format!(
            "{}/api/metalakes/{}/catalogs/{}",
            self.gravitino_uri, self.metalake, catalog_name
        )
    }

    pub async fn get_catalog(&self, catalog_name: &str) -> Result<Catalog, GvfsError> {
        let url = self.get_catalog_url(catalog_name).await;
        let res = self.do_get::<CatalogResponse>(&url).await?;

        if res.code != 0 {
            return Err(GvfsError::Error(
                ErrorCode::GravitinoClientError,
                "Failed to get catalog".to_string(),
            ));
        }
        Ok(res.catalog)
    }

    pub async fn get_fileset(
        &self,
        catalog_name: &str,
        schema_name: &str,
        fileset_name: &str,
    ) -> Result<Fileset, GvfsError> {
        let url = self.get_fileset_url(catalog_name, schema_name, fileset_name);
        let res = self.do_get::<FilesetResponse>(&url).await?;

        if res.code != 0 {
            return Err(GvfsError::Error(
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
            self.gravitino_uri,
            self.metalake,
            catalog_name,
            schema_name,
            fileset_name,
            encoded_path
        )
    }

    pub async fn get_file_location(
        &self,
        catalog_name: &str,
        schema_name: &str,
        fileset_name: &str,
        path: &str,
    ) -> Result<String, GvfsError> {
        let url = self.get_file_location_url(catalog_name, schema_name, fileset_name, path);
        let res = self.do_get::<FileLocationResponse>(&url).await?;

        if res.code != 0 {
            return Err(GvfsError::Error(
                ErrorCode::GravitinoClientError,
                "Failed to get file location".to_string(),
            ));
        }
        Ok(res.location)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use mockito::mock;
    use tracing_subscriber::EnvFilter;

    pub(crate) fn create_test_catalog(
        name: &str,
        provider: &str,
        properties: HashMap<String, String>,
    ) -> Catalog {
        Catalog {
            name: name.to_string(),
            catalog_type: "fileset".to_string(),
            provider: provider.to_string(),
            comment: "".to_string(),
            properties: properties,
        }
    }

    pub(crate) fn create_test_fileset(name: &str, storage_location: &str) -> Fileset {
        Fileset {
            name: name.to_string(),
            fileset_type: "managed".to_string(),
            comment: "".to_string(),
            storage_location: storage_location.to_string(),
            properties: HashMap::default(),
        }
    }

    #[tokio::test]
    async fn test_get_fileset_success() {
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
            uri: mock_server_url.to_string(),
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
        let file_location_response = r#"
        {
            "code": 0,
            "fileLocation": "/mybucket/a"
        }"#;

        let mock_server_url = &mockito::server_url();

        let url = format!(
            "/api/metalakes/{}/catalogs/{}/schemas/{}/filesets/{}/location?sub_path={}",
            "test",
            "catalog1",
            "schema1",
            "fileset1",
            encode("/example/path")
        );
        let _m = mock("GET", url.as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(file_location_response)
            .create();

        let config = GravitinoConfig {
            uri: mock_server_url.to_string(),
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

    #[tokio::test]
    async fn test_get_catalog_success() {
        let catalog_response = r#"
        {
            "code": 0,
            "catalog": {
                "name": "example_catalog",
                "type": "example_type",
                "provider": "example_provider",
                "comment": "This is a test catalog",
                "properties": {
                    "key1": "value1",
                    "key2": "value2"
                }
            }
        }"#;

        let mock_server_url = &mockito::server_url();

        let url = format!("/api/metalakes/{}/catalogs/{}", "test", "catalog1");
        let _m = mock("GET", url.as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(catalog_response)
            .create();

        let config = GravitinoConfig {
            uri: mock_server_url.to_string(),
            metalake: "test".to_string(),
        };
        let client = GravitinoClient::new(&config);

        let result = client.get_catalog("catalog1").await;

        match result {
            Ok(_) => {}
            Err(e) => panic!("Expected Ok, but got Err: {:?}", e),
        }
    }

    async fn get_fileset_example() {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();

        let config = GravitinoConfig {
            uri: "http://localhost:8090".to_string(),
            metalake: "test".to_string(),
        };
        let client = GravitinoClient::new(&config);
        client.init();
        let result = client.get_fileset("c1", "s1", "fileset1").await;
        if let Err(e) = &result {
            println!("{:?}", e);
        }

        let fileset = result.unwrap();
        println!("{:?}", fileset);
        assert_eq!(fileset.name, "fileset1");
    }
}
