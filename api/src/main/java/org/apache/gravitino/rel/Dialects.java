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
package org.apache.gravitino.rel;

/**
 * Predefined SQL dialect identifiers used by {@link SQLRepresentation#dialect()}.
 *
 * <p>Dialect values are lowercase identifiers so they can be compared case-insensitively.
 * Applications are free to use dialect strings that are not listed here; this class only provides
 * constants for the dialects Gravitino recognizes out of the box.
 */
public final class Dialects {

  /** The Trino SQL dialect. */
  public static final String TRINO = "trino";

  /** The Apache Spark SQL dialect. */
  public static final String SPARK = "spark";

  /** The Apache Hive SQL dialect. */
  public static final String HIVE = "hive";

  /** The Apache Flink SQL dialect. */
  public static final String FLINK = "flink";

  private Dialects() {}
}
