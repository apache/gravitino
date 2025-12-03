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
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;

/**
 * Utility class to convert Hive exceptions to Gravitino exceptions. This class handles various
 * types of exceptions that can be thrown by Hive Metastore operations, including:
 *
 * <ul>
 *   <li>Reflection exceptions (InvocationTargetException)
 *   <Li>Hive Metastore exceptions (e.g., AlreadyExistsException, NoSuchObjectException,
 *       InvalidOperationException, MetaException)
 *   <li>Hive Thrift exceptions (TException)
 *   <li>Other runtime exceptions
 * </ul>
 */
public class HiveExceptionConverter {

  private enum TargetType {
    TABLE,
    SCHEMA,
    PARTITION,
    CATALOG,
    OTHER
  }

  /** Represents the target Hive object (name + type) associated with an operation. */
  public static final class ExceptionTarget {
    private final String name;
    private final TargetType type;

    private ExceptionTarget(String name, TargetType type) {
      this.name = name;
      this.type = type;
    }

    public static ExceptionTarget table(String name) {
      return new ExceptionTarget(name, TargetType.TABLE);
    }

    public static ExceptionTarget schema(String name) {
      return new ExceptionTarget(name, TargetType.SCHEMA);
    }

    public static ExceptionTarget catalog(String name) {
      return new ExceptionTarget(name, TargetType.CATALOG);
    }

    public static ExceptionTarget partition(String name) {
      return new ExceptionTarget(name, TargetType.PARTITION);
    }

    public static ExceptionTarget other(String name) {
      return new ExceptionTarget(name, TargetType.OTHER);
    }

    public String name() {
      return name;
    }

    public TargetType type() {
      return type;
    }
  }

  private HiveExceptionConverter() {}

  /**
   * Converts a generic exception to a Gravitino exception with a target Hive object.
   *
   * @param e The exception to convert
   * @param target The Hive object related to the operation (table, partition, schema, etc.)
   * @return A Gravitino exception
   */
  public static RuntimeException toGravitinoException(Exception e, ExceptionTarget target) {
    Throwable cause = unwrapException(e);
    return convertException(cause, target);
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
   * @param target The target Hive object of the operation
   * @return A Gravitino exception
   */
  private static RuntimeException convertException(Throwable cause, ExceptionTarget target) {
    if (cause instanceof RuntimeException && cause.getCause() instanceof Exception) {
      return toGravitinoException((Exception) cause.getCause(), target);
    }

    String message = cause.getMessage();
    String lowerMessage = message != null ? message.toLowerCase(Locale.ROOT) : "";
    String exceptionClassName = cause.getClass().getName();

    if (exceptionClassName.contains("AlreadyExistsException")) {
      return toAlreadyExistsException(cause, target, message);
    }
    if (exceptionClassName.contains("NoSuchObjectException")
        || exceptionClassName.contains("UnknownTableException")
        || exceptionClassName.contains("UnknownDBException")
        || exceptionClassName.contains("UnknownPartitionException")
        || exceptionClassName.contains("InvalidObjectException")
        || exceptionClassName.contains("InvalidPartitionException")) {
      return toNoSuchObjectException(cause, target, message);
    }
    if (exceptionClassName.contains("InvalidOperationException")) {
      if (isNonEmptySchemaMessage(lowerMessage)) {
        return new NonEmptySchemaException(
            cause, "Hive schema %s is not empty in Hive Metastore", target.name());
      }
      return new IllegalArgumentException(cause.getMessage(), cause);
    }

    if (exceptionClassName.contains("MetaException")) {
      if (lowerMessage.contains("invalid partition key")) {
        return new NoSuchPartitionException(
            cause, "Hive partition %s does not exist in Hive Metastore", target.name());
      }
    }

    if (exceptionClassName.contains("TException")) {
      if (lowerMessage.contains("already exists")) {
        return toAlreadyExistsException(cause, target, message);
      }
      if (lowerMessage.contains("does not exist")
          || lowerMessage.contains("not found")
          || lowerMessage.contains("no such")
          || lowerMessage.contains("there is no")
          || lowerMessage.contains("invalid partition")) {
        return toNoSuchObjectException(cause, target, message);
      }
      if (isNonEmptySchemaMessage(lowerMessage)) {
        return new NonEmptySchemaException(
            cause, "Hive schema %s is not empty in Hive Metastore", target.name());
      }
    }

    if (isConnectionKeyword(lowerMessage)) {
      return new ConnectionFailedException(
          cause, "Failed to connect to Hive Metastore: %s", target.name());
    }

    if (cause instanceof RuntimeException) {
      return (RuntimeException) cause;
    }
    return new GravitinoRuntimeException(cause, message);
  }

  private static boolean isConnectionKeyword(String lowerMessage) {
    return lowerMessage.contains("connection")
        || lowerMessage.contains("connect")
        || lowerMessage.contains("timeout")
        || lowerMessage.contains("network");
  }

  private static boolean isNonEmptySchemaMessage(String lowerMessage) {
    return (lowerMessage.contains("non-empty") || lowerMessage.contains("not empty"))
        && (lowerMessage.contains("schema") || lowerMessage.contains("database"));
  }

  private static RuntimeException toAlreadyExistsException(
      Throwable cause, ExceptionTarget target, String rawMessage) {
    TargetType objectType = target.type();
    return switch (objectType) {
      case PARTITION -> new PartitionAlreadyExistsException(
          cause, "Hive partition %s already exists in Hive Metastore", target.name());
      case TABLE -> new TableAlreadyExistsException(
          cause, "Hive table %s already exists in Hive Metastore", target.name());
      case SCHEMA -> new SchemaAlreadyExistsException(
          cause, "Hive schema %s already exists in Hive Metastore", target.name());
      default -> new AlreadyExistsException(cause, "%s", rawMessage);
    };
  }

  private static RuntimeException toNoSuchObjectException(
      Throwable cause, ExceptionTarget target, String rawMessage) {
    return switch (target.type()) {
      case PARTITION -> new NoSuchPartitionException(
          cause, "Hive partition %s does not exist in Hive Metastore", target.name());
      case TABLE -> new NoSuchTableException(
          cause, "Hive table %s does not exist in Hive Metastore", target.name());
      case SCHEMA -> new NoSuchSchemaException(
          cause, "Hive schema %s does not exist in Hive Metastore", target.name());
      default -> new NoSuchEntityException(cause, "%s", rawMessage);
    };
  }
}
