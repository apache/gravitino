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

import java.util.function.Function;
import javax.annotation.Nullable;
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

/** Converts Apache Fluss exceptions to Gravitino exceptions. */
final class FlussExceptionConverter {

  static final String SCHEMA_DOES_NOT_EXIST_MESSAGE = "Schema %s does not exist";
  static final String SCHEMA_ALREADY_EXISTS_MESSAGE = "Schema %s already exists";
  static final String SCHEMA_IS_NOT_EMPTY_MESSAGE = "Schema %s is not empty";
  static final String TABLE_DOES_NOT_EXIST_MESSAGE = "Table %s does not exist";
  static final String TABLE_ALREADY_EXISTS_MESSAGE = "Table %s already exists";
  static final String PARTITION_DOES_NOT_EXIST_MESSAGE = "Partition %s does not exist in table %s";
  static final String PARTITION_ALREADY_EXISTS_MESSAGE = "Partition %s already exists in table %s";
  static final String TABLE_IS_NOT_PARTITIONED_MESSAGE = "Table is not partitioned: %s";

  private FlussExceptionConverter() {}

  static Function<Throwable, RuntimeException> generic(String failureMessage) {
    return e -> new GravitinoRuntimeException(e, failureMessage);
  }

  static Function<Throwable, RuntimeException> forSchema(Object schema, String failureMessage) {
    return e -> {
      if (e instanceof DatabaseNotExistException) {
        return new NoSuchSchemaException(e, SCHEMA_DOES_NOT_EXIST_MESSAGE, schema);
      }
      if (e instanceof DatabaseAlreadyExistException) {
        return new SchemaAlreadyExistsException(e, SCHEMA_ALREADY_EXISTS_MESSAGE, schema);
      }
      if (e instanceof DatabaseNotEmptyException) {
        return new NonEmptySchemaException(e, SCHEMA_IS_NOT_EMPTY_MESSAGE, schema);
      }
      return new GravitinoRuntimeException(e, failureMessage);
    };
  }

  static Function<Throwable, RuntimeException> forTable(
      NameIdentifier table, String failureMessage) {
    return e -> {
      if (e instanceof DatabaseNotExistException) {
        return new NoSuchSchemaException(e, SCHEMA_DOES_NOT_EXIST_MESSAGE, table.namespace());
      }
      if (e instanceof TableNotExistException) {
        return new NoSuchTableException(e, TABLE_DOES_NOT_EXIST_MESSAGE, table);
      }
      if (e instanceof TableAlreadyExistException) {
        return new TableAlreadyExistsException(e, TABLE_ALREADY_EXISTS_MESSAGE, table);
      }
      return new GravitinoRuntimeException(e, failureMessage);
    };
  }

  static Function<Throwable, RuntimeException> forPartition(
      TablePath tablePath, String failureMessage) {
    return forPartition(tablePath, null, failureMessage);
  }

  static Function<Throwable, RuntimeException> forPartition(
      TablePath tablePath, @Nullable String partitionName, String failureMessage) {
    return e -> {
      if (e instanceof TableNotExistException) {
        return new NoSuchTableException(e, TABLE_DOES_NOT_EXIST_MESSAGE, tablePath);
      }
      if (e instanceof PartitionNotExistException) {
        return new NoSuchPartitionException(
            e, PARTITION_DOES_NOT_EXIST_MESSAGE, partitionName, tablePath);
      }
      if (e instanceof org.apache.fluss.exception.PartitionAlreadyExistsException) {
        return new PartitionAlreadyExistsException(
            e, PARTITION_ALREADY_EXISTS_MESSAGE, partitionName, tablePath);
      }
      if (e instanceof TableNotPartitionedException) {
        return new UnsupportedOperationException(
            String.format(TABLE_IS_NOT_PARTITIONED_MESSAGE, tablePath), e);
      }
      return new GravitinoRuntimeException(e, failureMessage);
    };
  }
}
