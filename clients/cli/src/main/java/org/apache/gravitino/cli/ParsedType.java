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

package org.apache.gravitino.cli;

/**
 * Represents a parsed data type, encapsulating its name and attributes like length, precision, and
 * scale.
 *
 * <p>This class is a data container used to hold the components of a data type string that has been
 * parsed from a format like "varchar(10)" or "decimal(10,5)". It provides methods to access the
 * extracted type name and its associated attributes, which are mutually exclusive (i.e., a type has
 * either a length or a precision/scale, but not both).
 */
public class ParsedType {
  private String typeName;
  private Integer length;
  private Integer precision;
  private Integer scale;

  /**
   * Constructs a ParsedType with specified type name, precision, and scale.
   *
   * @param typeName The name of the data type.
   * @param precision The precision of the data type, which defines the total number of digits.
   * @param scale The scale of the data type, which defines the number of digits to the right of the
   *     decimal point.
   */
  public ParsedType(String typeName, Integer precision, Integer scale) {
    this.typeName = typeName;
    this.precision = precision;
    this.scale = scale;
  }

  /**
   * Constructs a ParsedType with specified type name and length.
   *
   * @param typeName The name of the data type.
   * @param length The length of the data type, which typically defines the maximum number of
   *     characters.
   */
  public ParsedType(String typeName, Integer length) {
    this.typeName = typeName;
    this.length = length;
  }

  /**
   * Gets the type name (e.g., "varchar" or "decimal").
   *
   * @return the type name
   */
  public String getTypeName() {
    return typeName;
  }

  /**
   * Gets the length for types like "varchar".
   *
   * @return the length, or null if not applicable
   */
  public Integer getLength() {
    return length;
  }

  /**
   * Gets the precision for types like "decimal".
   *
   * @return the precision, or null if not applicable
   */
  public Integer getPrecision() {
    return precision;
  }

  /**
   * Gets the scale for types like "decimal".
   *
   * @return the scale, or null if not applicable
   */
  public Integer getScale() {
    return scale;
  }

  /** Returns a string representation of this {@code ParsedType} object. */
  @Override
  public String toString() {
    if (length != null) {
      return String.format("Type: %s, Length: %d", typeName, length);
    } else if (precision != null && scale != null) {
      return String.format("Type: %s, Precision: %d, Scale: %d", typeName, precision, scale);
    } else {
      return "Unsupported type";
    }
  }
}
