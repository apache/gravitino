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

package org.apache.gravitino.lance.common.utils;

public class LanceConstants {
  public static final String LANCE_HTTP_HEADER_PREFIX = "x-lance-";

  public static final String LANCE_TABLE_LOCATION_HEADER =
      LANCE_HTTP_HEADER_PREFIX + "table-location";
  public static final String LANCE_TABLE_PROPERTIES_PREFIX_HEADER =
      LANCE_HTTP_HEADER_PREFIX + "table-properties";
  // Key for table location in table properties map
  public static final String LANCE_LOCATION = "location";

  // Prefix for storage options in LanceConfig
  public static final String LANCE_STORAGE_OPTIONS_PREFIX = "lance.storage.";
}
