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
package org.apache.gravitino.iceberg.common.ops;

import java.util.Map;

/**
 * IcebergTableOpsProvider is an interface defining how Iceberg REST catalog server gets Iceberg
 * catalogs.
 */
public interface IcebergTableOpsProvider {

  /**
   * @param properties The parameters for creating Provider which from configurations whose prefix
   *     is 'gravitino.iceberg-rest.'
   */
  void initialize(Map<String, String> properties);

  /**
   * @param catalogName a param send by clients.
   * @return the instance of IcebergTableOps.
   */
  IcebergTableOps getIcebergTableOps(String catalogName);
}
