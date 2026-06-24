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
package org.apache.gravitino.lance.common.ops.hive;

/** Error types for the Hive Lance namespace backend. */
enum HiveErrorType {
  /** A failure interacting with the Hive Metastore. */
  HIVE_META_STORE_ERROR("HiveMetaStoreError"),
  /** A database (namespace) already exists. */
  DATABASE_ALREADY_EXIST("DatabaseAlreadyExist"),
  /** A table already exists. */
  TABLE_ALREADY_EXISTS("TableAlreadyExists"),
  /** A table was not found. */
  TABLE_NOT_FOUND("TableNotFound"),
  /** The table is not a valid Lance table. */
  INVALID_LANCE_TABLE("InvalidLanceTable");

  private final String type;

  HiveErrorType(String type) {
    this.type = type;
  }

  String getType() {
    return type;
  }
}
