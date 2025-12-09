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

/**
 * Utility class to convert Hive exceptions to Gravitino exceptions. This class handles various
 * types of exceptions that can be thrown by Hive Metastore operations, including:
 *
 * <ul>
 *   <li>Reflection exceptions (InvocationTargetException)
 *   <li>Hive Metastore exceptions (e.g., AlreadyExistsException, NoSuchObjectException,
 *       InvalidOperationException, MetaException)
 *   <li>Hive Thrift exceptions (TException)
 *   <li>Other runtime exceptions
 * </ul>
 */
public class HiveExceptionConverter {

  enum TargetType {
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
    return null;
  }
}
