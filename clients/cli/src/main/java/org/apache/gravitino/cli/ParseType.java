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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.rel.types.Type;

public class ParseType {

  /**
   * Parses a data type string and returns a {@link org.apache.gravitino.cli.ParsedType} object
   * containing the type name and length or type name, precision, and scale if applicable.
   *
   * <p>This method supports SQL style types in the format of "typeName(length)" or
   * "typeName(precision, scale)". For example, "varchar(10)" and "decimal(10,5)" are valid inputs.
   *
   * @param datatype The data type string to parse e.g. "varchar(10)" or "decimal(10,5)".
   * @return a {@link org.apache.gravitino.cli.ParsedType} object representing the parsed type name.
   * @throws IllegalArgumentException if the data type format is unsupported or malformed
   */
  public static ParsedType parse(String datatype) {
    Pattern pattern = Pattern.compile("^(\\w+)\\((\\d+)(?:,(\\d+))?\\)$");
    Matcher matcher = pattern.matcher(datatype);

    if (matcher.matches()) {
      String typeName = matcher.group(1);
      Integer lengthOrPrecision = Integer.parseInt(matcher.group(2));
      Integer scale = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : null;

      if (lengthOrPrecision != null && scale != null) {
        return new ParsedType(typeName, lengthOrPrecision, scale);
      } else if (lengthOrPrecision != null) {
        return new ParsedType(typeName, lengthOrPrecision);
      } else {
        throw new IllegalArgumentException("Unsupported/malformed data type: " + typeName);
      }
    }

    return null;
  }

  public static Type toType(String datatype) {
    ParsedType parsed = parse(datatype);

    if (parsed != null) {
      if (parsed.getPrecision() != null && parsed.getScale() != null) {
        return TypeConverter.convert(datatype, parsed.getPrecision(), parsed.getScale());
      } else if (parsed.getLength() != null) {
        return TypeConverter.convert(datatype, parsed.getLength());
      }
    }

    return TypeConverter.convert(datatype);
  }
}
