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

package org.apache.gravitino.catalog.fluss;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.function.Function;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.metadata.TablePath;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.junit.jupiter.api.Test;

class TestFlussExceptionConverter {

  @Test
  void testSchemaExceptionMappings() {
    Function<Throwable, RuntimeException> mapper =
        FlussExceptionConverter.forSchema("db", "Failed to operate schema");

    assertInstanceOf(
        NoSuchSchemaException.class, mapper.apply(new DatabaseNotExistException("db")));
    assertInstanceOf(
        SchemaAlreadyExistsException.class, mapper.apply(new DatabaseAlreadyExistException("db")));
    assertInstanceOf(
        NonEmptySchemaException.class, mapper.apply(new DatabaseNotEmptyException("db")));
    assertInstanceOf(
        GravitinoRuntimeException.class, mapper.apply(new IllegalStateException("failed")));
  }

  @Test
  void testTableExceptionMappings() {
    NameIdentifier table = NameIdentifier.of("metalake", "catalog", "db", "orders");
    Function<Throwable, RuntimeException> mapper =
        FlussExceptionConverter.forTable(table, "Failed to operate table");

    assertInstanceOf(
        NoSuchSchemaException.class, mapper.apply(new DatabaseNotExistException("db")));
    assertInstanceOf(
        NoSuchTableException.class, mapper.apply(new TableNotExistException("orders")));
    assertInstanceOf(
        TableAlreadyExistsException.class, mapper.apply(new TableAlreadyExistException("orders")));
    assertInstanceOf(
        GravitinoRuntimeException.class, mapper.apply(new IllegalStateException("failed")));
  }

  @Test
  void testPartitionExceptionMappings() {
    TablePath tablePath = TablePath.of("db", "orders");
    Function<Throwable, RuntimeException> mapper =
        FlussExceptionConverter.forPartition(
            tablePath, "event_day=20250405", "Failed to operate partition");

    assertInstanceOf(
        NoSuchTableException.class, mapper.apply(new TableNotExistException("orders")));
    assertInstanceOf(
        NoSuchPartitionException.class, mapper.apply(new PartitionNotExistException("partition")));
    assertInstanceOf(
        PartitionAlreadyExistsException.class,
        mapper.apply(new org.apache.fluss.exception.PartitionAlreadyExistsException("partition")));
    assertInstanceOf(
        UnsupportedOperationException.class,
        mapper.apply(new TableNotPartitionedException("not partitioned")));
    assertInstanceOf(
        GravitinoRuntimeException.class, mapper.apply(new IllegalStateException("failed")));
  }

  @Test
  void testGenericExceptionMapping() {
    RuntimeException exception =
        FlussExceptionConverter.generic("Failed to execute").apply(new IllegalStateException());

    assertInstanceOf(GravitinoRuntimeException.class, exception);
  }
}
