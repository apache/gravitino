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
package org.apache.gravitino.spark.connector.jdbc.doris;

import com.google.common.collect.ImmutableSet;
import java.util.EnumSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.TableCapability;

/** Central capability gate that prevents official Doris write capabilities from leaking. */
final class DorisCapabilityPolicy35 {

  private static final DorisCapabilityPolicy35 READ_ONLY =
      new DorisCapabilityPolicy35(ImmutableSet.of(TableCapability.BATCH_READ));

  private final Set<TableCapability> tableCapabilities;

  private DorisCapabilityPolicy35(Set<TableCapability> tableCapabilities) {
    this.tableCapabilities = ImmutableSet.copyOf(tableCapabilities);
  }

  static DorisCapabilityPolicy35 readOnly() {
    return READ_ONLY;
  }

  static DorisCapabilityPolicy35 from(DorisWritePolicy35 writePolicy) {
    if (!writePolicy.enabled()) {
      return readOnly();
    }
    // Keep the specialized Doris surface narrower than generic JDBC: every write capability is
    // explicitly enabled by the catalog policy instead of being inherited from the delegate.
    EnumSet<TableCapability> capabilities =
        EnumSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
    if (writePolicy.allowsTruncate()) {
      capabilities.add(TableCapability.TRUNCATE);
    }
    return new DorisCapabilityPolicy35(capabilities);
  }

  Set<TableCapability> tableCapabilities() {
    return tableCapabilities;
  }

  boolean allowsTableWrites() {
    return tableCapabilities.contains(TableCapability.BATCH_WRITE);
  }

  UnsupportedOperationException reject(String operation) {
    return new UnsupportedOperationException(
        "The governed Doris connector policy does not support " + operation);
  }
}
