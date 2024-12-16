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
package org.apache.gravitino.storage;

// Defines unified properties for Azure configurations.
public class AzureProperties {

  // Configuration key for specifying the name of the storage account.
  public static final String GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME = "azure-storage-account-name";
  // Configuration key for specifying the key of the storage account.
  public static final String GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY = "azure-storage-account-key";

  // Configuration key for specifying the Azure Active Directory (AAD) tenant ID.
  public static final String GRAVITINO_AZURE_TENANT_ID = "azure-tenant-id";
  // Configuration key for specifying the Azure Active Directory (AAD) client ID used for
  // authentication.
  public static final String GRAVITINO_AZURE_CLIENT_ID = "azure-client-id";
  // Configuration key for specifying the Azure Active Directory (AAD) client secret used for
  // authentication.
  public static final String GRAVITINO_AZURE_CLIENT_SECRET = "azure-client-secret";

  private AzureProperties() {}
}
