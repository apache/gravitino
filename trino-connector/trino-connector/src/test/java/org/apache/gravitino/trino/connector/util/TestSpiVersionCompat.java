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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.type.BigintType;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SpiVersionCompat}. Because the shared test source is compiled and run by
 * every version-segment module, these tests pin the cross-version contract of the seam: {@link
 * SpiVersionCompat#columnComment} runs against the {@code String}-returning {@code
 * ColumnMetadata.getComment()} of Trino 479 and the {@code Optional}-returning one of Trino 480+,
 * and the {@code SchemaFunctionName} accessors against both the {@code get*} and record-accessor
 * shapes, each asserting identical behavior.
 */
class TestSpiVersionCompat {

  /** Helper target for the generic {@link SpiVersionCompat#invoke} tests. */
  public static class ReflectiveTarget {
    public String echo(String value) {
      return "echo:" + value;
    }

    public String describe() {
      return "no-arg";
    }

    public String describe(int value) {
      return "int:" + value;
    }

    public void boom() {
      throw new IllegalArgumentException("boom");
    }
  }

  @Test
  void testColumnCommentReturnsComment() {
    ColumnMetadata column =
        ColumnMetadata.builder()
            .setName("c1")
            .setType(BigintType.BIGINT)
            .setComment(Optional.of("a comment"))
            .build();

    assertThat(SpiVersionCompat.columnComment(column)).isEqualTo("a comment");
  }

  @Test
  void testColumnCommentReturnsNullWhenAbsent() {
    ColumnMetadata column =
        ColumnMetadata.builder()
            .setName("c1")
            .setType(BigintType.BIGINT)
            .setComment(Optional.empty())
            .build();

    assertThat(SpiVersionCompat.columnComment(column)).isNull();
  }

  @Test
  void testSchemaAndFunctionName() {
    SchemaFunctionName name = new SchemaFunctionName("my_schema", "my_func");

    assertThat(SpiVersionCompat.schemaName(name)).isEqualTo("my_schema");
    assertThat(SpiVersionCompat.functionName(name)).isEqualTo("my_func");
  }

  @Test
  void testInvokeCallsMethod() {
    ReflectiveTarget target = new ReflectiveTarget();

    Object result = SpiVersionCompat.invoke(target, "echo", new Class<?>[] {String.class}, "hello");

    assertThat(result).isEqualTo("echo:hello");
  }

  @Test
  void testInvokeUsesCachedMethodAcrossRepeatedCalls() {
    ReflectiveTarget target = new ReflectiveTarget();

    // Repeated calls with the same (class, name, parameter types) go through the cached lookup and
    // must keep returning the same result.
    assertThat(SpiVersionCompat.invoke(target, "echo", new Class<?>[] {String.class}, "a"))
        .isEqualTo("echo:a");
    assertThat(SpiVersionCompat.invoke(target, "echo", new Class<?>[] {String.class}, "b"))
        .isEqualTo("echo:b");
  }

  @Test
  void testInvokeDistinguishesOverloadsByParameterTypes() {
    ReflectiveTarget target = new ReflectiveTarget();

    assertThat(SpiVersionCompat.invoke(target, "describe", new Class<?>[] {})).isEqualTo("no-arg");
    assertThat(SpiVersionCompat.invoke(target, "describe", new Class<?>[] {int.class}, 7))
        .isEqualTo("int:7");
  }

  @Test
  void testInvokeThrowsForMissingMethod() {
    ReflectiveTarget target = new ReflectiveTarget();

    assertThatThrownBy(() -> SpiVersionCompat.invoke(target, "doesNotExist", new Class<?>[] {}))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("was not found");
  }

  @Test
  void testInvokeWrapsTargetException() {
    ReflectiveTarget target = new ReflectiveTarget();

    assertThatThrownBy(() -> SpiVersionCompat.invoke(target, "boom", new Class<?>[] {}))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed invoking Trino SPI method");
  }
}
