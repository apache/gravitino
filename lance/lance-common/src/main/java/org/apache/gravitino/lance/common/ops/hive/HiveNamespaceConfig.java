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

/** Property keys used to map Hive database metadata to Lance namespace properties. */
final class HiveNamespaceConfig {

  /** Property key carrying the Hive database description. */
  static final String DATABASE_DESCRIPTION = "database.description";
  /** Property key carrying the Hive database location URI. */
  static final String DATABASE_LOCATION_URI = "database.location-uri";
  /** Property key carrying the Hive database owner name. */
  static final String DATABASE_OWNER = "owner";
  /** Property key carrying the Hive database owner type. */
  static final String DATABASE_OWNER_TYPE = "owner_type";

  private HiveNamespaceConfig() {}
}
