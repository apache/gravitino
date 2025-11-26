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
 *   <li>Hive Meta exceptions (MetaException, AlreadyExistsException, InvalidObjectException,
 *       NoSuchObjectException)
 *   <li>Other runtime exceptions
 * </ul>
 */
public class HiveExceptionConverter {

  private HiveExceptionConverter() {}

  /**
   * Converts a generic exception to a Gravitino exception.
   *
   * @param e The exception to convert
   * @return A Gravitino exception
   */
  public static RuntimeException toGravitinoException(Exception e) {
    Throwable cause = unwrapException(e);
    return convertException(cause);
  }

  /**
   * Unwraps nested exceptions, especially InvocationTargetException from reflection calls.
   *
   * @param e The exception to unwrap
   * @return The unwrapped exception
   */
  private static Throwable unwrapException(Exception e) {
    Throwable cause = e;
    // Unwrap InvocationTargetException from reflection calls
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
  private static RuntimeException convertException(Throwable cause) {
    String message = cause.getMessage();
    String lowerMessage = message != null ? message.toLowerCase() : "";

    // Handle Hive MetaException and related exceptions
    String exceptionClassName = cause.getClass().getName();

    // Check for AlreadyExistsException
    if (exceptionClassName.contains("AlreadyExistsException")) {
      // Try to determine type from message
      if (lowerMessage.contains("partition")) {
        return new PartitionAlreadyExistsException(
            cause, "Partition already exists in Hive Metastore");
      } else if (lowerMessage.contains("table")) {
        return new TableAlreadyExistsException(cause, "Table already exists in Hive Metastore");
      } else {
        return new SchemaAlreadyExistsException(cause, "Schema already exists in Hive Metastore");
      }
    }

    // Check for NoSuchObjectException
    if (exceptionClassName.contains("NoSuchObjectException")) {
      if (lowerMessage.contains("partition")) {
        return new NoSuchPartitionException(cause, "");
      } else if (lowerMessage.contains("table")) {
        return new NoSuchTableException(cause, "");
      } else {
        return new NoSuchSchemaException(cause, "");
      }
    }

    // Check for InvalidObjectException (often indicates non-empty schema)
    if (exceptionClassName.contains("InvalidObjectException")) {
      if (message != null && (message.contains("non-empty") || message.contains("not empty"))) {
        return new NonEmptySchemaException(cause, "");
      }
    }

    if (exceptionClassName.contains("InvalidArgumentException")) {
      // This can indicate various issues; default to GravitinoRuntimeException
      return new GravitinoRuntimeException(cause, "");
    }

    // Handle TException (Thrift exceptions)
    if (cause instanceof TException) {
      if (message != null) {
        if (lowerMessage.contains("already exists")) {
          if (lowerMessage.contains("partition")) {
            return new PartitionAlreadyExistsException(cause, "");
          } else if (lowerMessage.contains("table")) {
            return new TableAlreadyExistsException(cause, "");
          } else {
            return new SchemaAlreadyExistsException(cause, "");
          }
        }
        if (lowerMessage.contains("does not exist")
            || lowerMessage.contains("not found")
            || lowerMessage.contains("no such")) {
          if (lowerMessage.contains("partition")) {
            return new NoSuchPartitionException(cause, "");
          } else if (lowerMessage.contains("table")) {
            return new NoSuchTableException(cause, "");
          } else {
            return new NoSuchSchemaException(cause, "");
          }
        }
        if (lowerMessage.contains("connection")
            || lowerMessage.contains("connect")
            || lowerMessage.contains("timeout")
            || lowerMessage.contains("network")) {
          return new ConnectionFailedException(cause, "");
        }
        if (lowerMessage.contains("non-empty") || lowerMessage.contains("not empty")) {
          return new NonEmptySchemaException(cause, "");
        }
      }
      return new GravitinoRuntimeException(cause, "");
    }

    // Handle MetaException
    if (exceptionClassName.contains("MetaException")) {
      if (message != null) {
        if (lowerMessage.contains("connection")
            || lowerMessage.contains("connect")
            || lowerMessage.contains("timeout")) {
          return new ConnectionFailedException(cause, "");
        }
      }
      return new GravitinoRuntimeException(cause, "");
    }

    if (cause instanceof RuntimeException) {
      if (cause.getCause() != null && cause.getCause() instanceof Exception) {
        return toGravitinoException((Exception) cause.getCause());
      } else {
        return (RuntimeException) cause;
      }
    }

    // Default: wrap in GravitinoRuntimeException
    return new GravitinoRuntimeException(cause, message);
  }
}
