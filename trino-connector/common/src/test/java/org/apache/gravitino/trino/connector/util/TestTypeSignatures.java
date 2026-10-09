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

import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TypeSignatures}. Because the shared test source is compiled and run by
 * every version-segment module, these tests pin the cross-version contract of the seam: the shared
 * {@code getTypeSignature()}-shaped class of Trino 440-481 and the {@code
 * getTypeDescriptor()}-shaped module-local copy of the 482-483 segment must produce the same
 * signatures.
 */
class TestTypeSignatures {

  @Test
  void testSignatureReturnsSignature() {
    assertThat(TypeSignatures.signature(BigintType.BIGINT)).isEqualTo("bigint");
  }
}
