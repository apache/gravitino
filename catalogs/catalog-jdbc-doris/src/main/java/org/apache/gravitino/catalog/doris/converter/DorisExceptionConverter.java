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
package org.apache.gravitino.catalog.doris.converter;

import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.regex.Pattern;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.exceptions.UnauthorizedException;

/** Exception converter to Apache Gravitino exception for Apache Doris. */
public class DorisExceptionConverter extends JdbcExceptionConverter {

  // see: https://doris.apache.org/docs/admin-manual/maint-monitor/doris-error-code/
  @VisibleForTesting static final int CODE_DATABASE_EXISTS = 1007;

  static final int CODE_TABLE_EXISTS = 1050;
  static final int CODE_NO_SUCH_SCHEMA = 1049;
  static final int CODE_DATABASE_NOT_EXISTS = 1008;
  static final int CODE_UNKNOWN_DATABASE = 1049;
  static final int CODE_NO_SUCH_TABLE = 1051;
  static final int CODE_UNAUTHORIZED = 1045;
  static final int CODE_NO_SUCH_COLUMN = 1054;
  static final int CODE_OTHER = 1105;
  static final int CODE_DELETE_NON_EXISTING_PARTITION = 1507;
  static final int CODE_PARTITION_ALREADY_EXISTS = 1517;
  static final int CODE_BUCKETS_AUTO_NOT_SUPPORTED = 1064;

  private static final String DATABASE_ALREADY_EXISTS_PATTERN_STRING =
      ".*?detailMessage = Can't create database '.*?'; database exists";
  private static final Pattern DATABASE_ALREADY_EXISTS_PATTERN =
      Pattern.compile(DATABASE_ALREADY_EXISTS_PATTERN_STRING);

  private static final String DATABASE_NOT_EXISTS_PATTERN_STRING =
      ".*?detailMessage = Can't drop database '.*?'; database doesn't exist";
  private static final Pattern DATABASE_NOT_EXISTS_PATTERN =
      Pattern.compile(DATABASE_NOT_EXISTS_PATTERN_STRING);

  private static final String UNKNOWN_DATABASE_PATTERN_STRING =
      ".*?detailMessage = Unknown database '.*?'";
  private static final Pattern UNKNOWN_DATABASE_PATTERN_PATTERN =
      Pattern.compile(UNKNOWN_DATABASE_PATTERN_STRING);

  private static final String TABLE_NOT_EXIST_PATTERN_STRING =
      ".*detailMessage = Unknown table '.*' in .*:.*";

  private static final Pattern TABLE_NOT_EXIST_PATTERN =
      Pattern.compile(TABLE_NOT_EXIST_PATTERN_STRING);

  private static final String DELETE_NON_EXISTING_PARTITION_STRING =
      ".*?detailMessage = Error in list of partitions to .*?";

  private static final Pattern DELETE_NON_EXISTING_PARTITION =
      Pattern.compile(DELETE_NON_EXISTING_PARTITION_STRING);

  private static final String PARTITION_ALREADY_EXISTS_STRING =
      ".*?detailMessage = Duplicate partition name .*?";

  private static final Pattern PARTITION_ALREADY_EXISTS_PARTITION =
      Pattern.compile(PARTITION_ALREADY_EXISTS_STRING);

  private static final String BUCKETS_AUTO_NOT_SUPPORTED_STRING =
      ".*?syntax error.*AUTO.*|.*?You have an error in your SQL syntax.*AUTO.*|.*?errCode = 2, detailMessage = Syntax error.*AUTO.*";

  private static final Pattern BUCKETS_AUTO_NOT_SUPPORTED_PATTERN =
      Pattern.compile(BUCKETS_AUTO_NOT_SUPPORTED_STRING, Pattern.CASE_INSENSITIVE);

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException se) {
    int errorCode = se.getErrorCode();
    if (errorCode == CODE_OTHER) {
      errorCode = getErrorCodeFromMessage(se.getMessage());
    }

    switch (errorCode) {
      case CODE_DATABASE_EXISTS:
        return new SchemaAlreadyExistsException(se, se.getMessage());
      case CODE_TABLE_EXISTS:
        return new TableAlreadyExistsException(se, se.getMessage());
      case CODE_DATABASE_NOT_EXISTS:
      case CODE_UNKNOWN_DATABASE:
        return new NoSuchSchemaException(se, se.getMessage());
      case CODE_NO_SUCH_TABLE:
        return new NoSuchTableException(se, se.getMessage());
      case CODE_UNAUTHORIZED:
        return new UnauthorizedException(se, se.getMessage());
      case CODE_NO_SUCH_COLUMN:
        return new NoSuchColumnException(se, se.getMessage());
      case CODE_DELETE_NON_EXISTING_PARTITION:
        return new NoSuchPartitionException(se, se.getMessage());
      case CODE_PARTITION_ALREADY_EXISTS:
        return new PartitionAlreadyExistsException(se, se.getMessage());
      case CODE_BUCKETS_AUTO_NOT_SUPPORTED:
        String bucketsAutoMessage =
            String.format(
                "BUCKETS AUTO is not supported in this version of Apache Doris (requires Doris 1.2.2+). "
                    + "BUCKETS AUTO was introduced in Doris 1.2.2. "
                    + "Please either upgrade to Doris 1.2.2+ or specify a specific bucket number instead of AUTO. "
                    + "Original error: %s",
                se.getMessage());
        return new GravitinoRuntimeException(se, bucketsAutoMessage);
      default:
        if (se.getMessage() != null && se.getMessage().contains("Access denied")) {
          return new ConnectionFailedException(se, se.getMessage());
        }
        return new GravitinoRuntimeException(se, se.getMessage());
    }
  }

  @VisibleForTesting
  static int getErrorCodeFromMessage(String message) {
    if (message.isEmpty()) {
      return CODE_OTHER;
    }
    if (DATABASE_ALREADY_EXISTS_PATTERN.matcher(message).matches()) {
      return CODE_DATABASE_EXISTS;
    }

    if (DATABASE_NOT_EXISTS_PATTERN.matcher(message).matches()) {
      return CODE_DATABASE_NOT_EXISTS;
    }

    if (UNKNOWN_DATABASE_PATTERN_PATTERN.matcher(message).matches()) {
      return CODE_UNKNOWN_DATABASE;
    }

    if (TABLE_NOT_EXIST_PATTERN.matcher(message).matches()) {
      return CODE_NO_SUCH_TABLE;
    }

    if (DELETE_NON_EXISTING_PARTITION.matcher(message).matches()) {
      return CODE_DELETE_NON_EXISTING_PARTITION;
    }

    if (PARTITION_ALREADY_EXISTS_PARTITION.matcher(message).matches()) {
      return CODE_PARTITION_ALREADY_EXISTS;
    }

    if (BUCKETS_AUTO_NOT_SUPPORTED_PATTERN.matcher(message).matches()) {
      return CODE_BUCKETS_AUTO_NOT_SUPPORTED;
    }

    return CODE_OTHER;
  }
}
