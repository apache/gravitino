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
package org.apache.gravitino.hive.client;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.thrift.TException;

/**
 * Utility class to convert Hive exceptions to Gravitino exceptions. This class handles various
 * types of exceptions that can be thrown by Hive Metastore operations, including:
 *
 * <ul>
 *   <li>Reflection exceptions (InvocationTargetException)
 *   <li>Hive Thrift exceptions (TException)
 *   <li>Other runtime exceptions
 * </ul>
 */
public class HiveExceptionConverter {

  private static final String UNKNOWN_TARGET = "[unknown]";

  private HiveExceptionConverter() {}

  /**
   * Converts a generic exception to a Gravitino exception with a target Hive object name.
   *
   * @param e The exception to convert
   * @param targetName The Hive object name related to the operation (table, partition, etc.)
   * @return A Gravitino exception
   */
  public static RuntimeException toGravitinoException(Exception e, String targetName) {
    Throwable cause = unwrapException(e);
    return convertException(cause, targetName);
  }

  /**
   * Unwraps nested exceptions, especially InvocationTargetException from reflection calls.
   *
   * @param e The exception to unwrap
   * @return The unwrapped exception
   */
  private static Throwable unwrapException(Exception e) {
    Throwable cause = e;
    if (e instanceof InvocationTargetException) {
      InvocationTargetException ite = (InvocationTargetException) e;
      cause = ite.getTargetException();
      if (cause == null) {
        cause = e;
      }
    }
    return cause;
  }

  /**
   * Converts the exception to the appropriate Gravitino exception based on its type.
   *
   * @param cause The exception cause
   * @return A Gravitino exception
   */
  private static RuntimeException convertException(Throwable cause, String targetName) {
    if (cause instanceof RuntimeException && cause.getCause() instanceof Exception) {
      return toGravitinoException((Exception) cause.getCause(), targetName);
    }

    String message = cause.getMessage();
    String lowerMessage = message != null ? message.toLowerCase(Locale.ROOT) : "";
    String exceptionClassName = cause.getClass().getName();

    if (exceptionClassName.contains("AlreadyExistsException")) {
      return toAlreadyExistsException(cause, lowerMessage, targetName, message);
    }
    if (exceptionClassName.contains("NoSuchObjectException")
        || exceptionClassName.contains("UnknownTableException")
        || exceptionClassName.contains("UnknownDBException")
        || exceptionClassName.contains("UnknownPartitionException")
        || exceptionClassName.contains("InvalidObjectException")
        || exceptionClassName.contains("InvalidPartitionException")) {
      return toNoSuchObjectException(cause, lowerMessage, targetName, message);
    }
    if (exceptionClassName.contains("InvalidOperationException")
        || exceptionClassName.contains("MetaException")) {
      if (isNonEmptySchemaMessage(lowerMessage)) {
        return new NonEmptySchemaException(
            cause, "Hive schema %s is not empty in Hive Metastore", targetName);
      }
    }

    if (exceptionClassName.contains("MetaException") && message != null) {
      if (message.contains("Invalid partition key")) {
        return new NoSuchPartitionException(
            cause, "Hive partition %s does not exist in Hive Metastore", targetName);
      }
    }

    if (cause instanceof TException && message != null) {
      if (lowerMessage.contains("already exists")) {
        return toAlreadyExistsException(cause, lowerMessage, targetName, message);
      }
      if (lowerMessage.contains("does not exist")
          || lowerMessage.contains("not found")
          || lowerMessage.contains("no such")
          || lowerMessage.contains("there is no")
          || lowerMessage.contains("invalid partition")) {
        return toNoSuchObjectException(cause, lowerMessage, targetName, message);
      }
      if (isNonEmptySchemaMessage(lowerMessage)) {
        return new NonEmptySchemaException(
            cause, "Hive schema %s is not empty in Hive Metastore", targetName);
      }

      if (isConnectionKeyword(lowerMessage)) {
        return new ConnectionFailedException(
            cause, "Failed to connect to Hive Metastore: %s", targetName);
      }
    }

    if (cause instanceof RuntimeException) {
      return (RuntimeException) cause;
    }
    return new GravitinoRuntimeException(cause, message);
  }

  private static boolean isConnectionKeyword(String lowerMessage) {
    if (lowerMessage == null) {
      return false;
    }
    return lowerMessage.contains("connection")
        || lowerMessage.contains("connect")
        || lowerMessage.contains("timeout")
        || lowerMessage.contains("network");
  }

  private static boolean isNonEmptySchemaMessage(String lowerMessage) {
    if (lowerMessage == null) {
      return false;
    }
    return (lowerMessage.contains("non-empty") || lowerMessage.contains("not empty"))
        && (lowerMessage.contains("schema") || lowerMessage.contains("database"));
  }

  private static RuntimeException toAlreadyExistsException(
      Throwable cause, String lowerMessage, String target, String rawMessage) {
    HiveObjectType objectType = detectObjectType(lowerMessage);
    return switch (objectType) {
      case PARTITION -> new PartitionAlreadyExistsException(
          cause, "Hive partition %s already exists in Hive Metastore", target);
      case TABLE -> new TableAlreadyExistsException(
          cause, "Hive table %s already exists in Hive Metastore", target);
      case SCHEMA -> new SchemaAlreadyExistsException(
          cause, "Hive schema %s already exists in Hive Metastore", target);
      default -> new GravitinoRuntimeException(cause, rawMessage);
    };
  }

  private static RuntimeException toNoSuchObjectException(
      Throwable cause, String lowerMessage, String target, String rawMessage) {
    HiveObjectType objectType = detectObjectType(lowerMessage);
    return switch (objectType) {
      case PARTITION -> new NoSuchPartitionException(
          cause, "Hive partition %s does not exist in Hive Metastore", target);
      case TABLE -> new NoSuchTableException(
          cause, "Hive table %s does not exist in Hive Metastore", target);
      case SCHEMA -> new NoSuchSchemaException(
          cause, "Hive schema %s does not exist in Hive Metastore", target);
      default -> new GravitinoRuntimeException(cause, rawMessage);
    };
  }

  private static HiveObjectType detectObjectType(String lowerMessage) {
    if (lowerMessage == null || lowerMessage.isEmpty()) {
      return HiveObjectType.UNKNOWN;
    }
    if (lowerMessage.contains("partition")) {
      return HiveObjectType.PARTITION;
    }
    if (lowerMessage.contains("table")) {
      return HiveObjectType.TABLE;
    }
    if (lowerMessage.contains("schema") || lowerMessage.contains("database")) {
      return HiveObjectType.SCHEMA;
    }
    return HiveObjectType.UNKNOWN;
  }

  private static boolean hasCause(Throwable throwable, Class<? extends Throwable> targetClass) {
    Throwable current = throwable;
    while (current != null) {
      if (targetClass.isInstance(current)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private enum HiveObjectType {
    TABLE,
    SCHEMA,
    PARTITION,
    UNKNOWN
  }
}
