/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.rel;

/** A generic table interface that extends the Table interface. */
public interface GenericTable extends Table {

  /**
   * Formats the table as a string representation.
   *
   * @return the formatted string representation of the table
   */
  String format();

  /**
   * Gets the location of the table.
   *
   * @return the location of the table
   */
  String location();

  /**
   * Indicates whether the table is external.
   *
   * @return true if the table is external, false otherwise
   */
  default boolean external() {
    return false;
  }
}
