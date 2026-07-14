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
package org.apache.gravitino.catalog;

import java.util.Locale;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCapabilityHelpers {

  private static final Capability UPPERCASE_CAPABILITY =
      new Capability() {
        @Override
        public CapabilityResult caseSensitiveOnName(Scope scope) {
          return CapabilityResult.unsupported("not case sensitive");
        }

        @Override
        public String normalizeName(Scope scope, String name) {
          return name.toUpperCase(Locale.ROOT);
        }
      };

  private static final Capability NULL_NORMALIZING_CAPABILITY =
      new Capability() {
        @Override
        public String normalizeName(Scope scope, String name) {
          return null;
        }
      };

  @Test
  void testApplyCaseSensitiveOnNameHonorsCustomNormalizeName() {
    String normalized =
        CapabilityHelpers.applyCaseSensitiveOnName(
            Capability.Scope.TABLE, "myTable", UPPERCASE_CAPABILITY);
    Assertions.assertEquals("MYTABLE", normalized);
  }

  @Test
  void testApplyCaseSensitivePartitionHonorsCustomNormalizeName() {
    IdentityPartition partition =
        Partitions.identity(
            "myPartition",
            new String[][] {{"col1"}},
            new Literal<?>[] {Literals.stringLiteral("val1")},
            null);

    Partition result = CapabilityHelpers.applyCaseSensitive(partition, UPPERCASE_CAPABILITY);
    Assertions.assertEquals("MYPARTITION", result.name());
  }

  @Test
  void testApplyCaseSensitiveOnNameRejectsNullNormalizeNameResult() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            CapabilityHelpers.applyCaseSensitiveOnName(
                Capability.Scope.TABLE, "myTable", NULL_NORMALIZING_CAPABILITY));
  }

  @Test
  void testApplyCaseSensitiveOnNameAllowsNullNameToPassThrough() {
    // A null name (e.g. an auto-generated identity partition name yet to be assigned) must be
    // allowed to pass through as null rather than being rejected as an invalid normalizeName
    // result.
    String normalized =
        CapabilityHelpers.applyCaseSensitiveOnName(
            Capability.Scope.PARTITION, null, Capability.DEFAULT);
    Assertions.assertNull(normalized);
  }
}
