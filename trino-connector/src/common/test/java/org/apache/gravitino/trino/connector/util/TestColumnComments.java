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
package org.apache.gravitino.trino.connector.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ColumnComments}. Every version module runs these tests, so the
 * String-shaped implementation of Trino 440-479 and the Optional-shaped implementations from Trino
 * 480 on must behave identically.
 */
class TestColumnComments {

  @Test
  void testReadReturnsComment() {
    ColumnMetadata column =
        ColumnMetadata.builder()
            .setName("c1")
            .setType(BigintType.BIGINT)
            .setComment(Optional.of("a comment"))
            .build();

    assertThat(ColumnComments.read(column)).isEqualTo("a comment");
  }

  @Test
  void testReadReturnsNullWhenAbsent() {
    ColumnMetadata column =
        ColumnMetadata.builder()
            .setName("c1")
            .setType(BigintType.BIGINT)
            .setComment(Optional.empty())
            .build();

    assertThat(ColumnComments.read(column)).isNull();
  }
}
