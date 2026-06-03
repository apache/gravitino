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
package org.apache.gravitino.connector;

import org.apache.gravitino.annotation.Evolving;

@Evolving
public enum TableCapability {
  /** Indicates whether the table format supports indexes. */
  SUPPORTS_INDEX,
  /** Indicates whether the table format supports partitioning. */
  SUPPORTS_PARTITIONING,
  /** Indicates whether the table format supports distribution definitions. */
  SUPPORTS_DISTRIBUTION,
  /** Indicates whether the table format supports sort orders. */
  SUPPORTS_SORT_ORDERS,
  /** Indicates whether the table format requires a non-empty table location. */
  REQUIRES_LOCATION
}
