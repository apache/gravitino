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
package org.apache.gravitino.cli.outputs;

import com.google.common.base.Joiner;

/**
 * Defines formatting behavior for command-line interface output. Implementations of this interface
 * handle the conversion of entities to their string representation in specific output formats.
 */
public interface OutputFormat<T> {
  /** Joiner for creating comma-separated output strings, ignoring null values */
  Joiner COMMA_JOINER = Joiner.on(",").skipNulls();
  /** Joiner for creating line-separated output strings, ignoring null values */
  Joiner NEWLINE_JOINER = Joiner.on(System.lineSeparator()).skipNulls();

  /**
   * Displays the entity in the specified output format. This method handles the actual output
   * operation
   *
   * @param entity The entity to be formatted and output
   */
  void output(T entity);

  /**
   * Returns entity's string representation. This method only handles the formatting without
   * performing any I/O operations.
   *
   * @param entity The entity to be formatted
   * @return The formatted string representation of the entity
   */
  String getOutput(T entity);
}
