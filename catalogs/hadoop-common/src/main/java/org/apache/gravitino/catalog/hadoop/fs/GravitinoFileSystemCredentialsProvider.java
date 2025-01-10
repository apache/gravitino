/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.hadoop.fs;

import org.apache.gravitino.credential.Credential;
import org.apache.hadoop.conf.Configurable;

/** Interface for providing credentials for Gravitino Virtual File System. */
public interface GravitinoFileSystemCredentialsProvider extends Configurable {

  String GVFS_CREDENTIAL_PROVIDER = "fs.gvfs.credential.provider";

  String GVFS_NAME_IDENTIFIER = "fs.gvfs.name.identifier";

  /**
   * Get credentials for Gravitino Virtual File System.
   *
   * @return credentials for Gravitino Virtual File System
   */
  Credential[] getCredentials();
}
