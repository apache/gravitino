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
package org.apache.gravitino.dto.rel;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;

/** Utility class for partitioning validation. */
public class PartitionUtils {

  /**
   * Validates the existence of the partition field in the table.
   *
   * @param columns The columns of the table.
   * @param fieldName The name of the field to validate.
   * @throws IllegalArgumentException If the field does not exist in the table, this exception is
   *     thrown.
   */
  public static void validateFieldExistence(ColumnDTO[] columns, String[] fieldName)
      throws IllegalArgumentException {
    Preconditions.checkArgument(ArrayUtils.isNotEmpty(columns), "columns cannot be null or empty");
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(fieldName), "fieldName cannot be null or empty");

    // Check if nested fields are supported (currently not supported)
    Preconditions.checkArgument(
        fieldName.length == 1,
        "Nested fields are not supported yet. Field name array must contain exactly one element, but got: %s",
        Arrays.toString(fieldName));

    List<ColumnDTO> partitionColumn =
        Arrays.stream(columns)
            // (TODO) Need to consider the case sensitivity issues.
            // To be optimized.
            .filter(c -> c.name().equalsIgnoreCase(fieldName[0]))
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        partitionColumn.size() == 1, "Field '%s' not found in table", fieldName[0]);

    // TODO: should validate nested fieldName after column type support namedStruct
  }

  private PartitionUtils() {}
}
