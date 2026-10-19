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
package org.apache.gravitino.lance.common.ops.gravitino;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility identifier parser for Lance namespace/table string IDs.
 *
 * <p>A Lance identifier is a single string whose levels are joined by a delimiter, for example
 * {@code catalog$schema}. An empty string denotes the root namespace.
 */
public class ObjectIdentifier {

  private final List<String> levels;

  private ObjectIdentifier(List<String> levels) {
    this.levels = levels;
  }

  /**
   * Parses a Lance identifier.
   *
   * @param id the identifier, an empty string denotes the root namespace.
   * @param delimiterRegex the regular expression matching the level delimiter.
   * @return the parsed identifier.
   */
  public static ObjectIdentifier of(String id, String delimiterRegex) {
    Preconditions.checkArgument(id != null, "Identifier cannot be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(delimiterRegex), "Delimiter regex cannot be blank");

    if (id.isEmpty()) {
      return new ObjectIdentifier(Collections.emptyList());
    }

    List<String> parsedLevels =
        Arrays.stream(id.split(delimiterRegex))
            .filter(StringUtils::isNotEmpty)
            .collect(Collectors.toList());
    return new ObjectIdentifier(parsedLevels);
  }

  /**
   * Returns the number of levels in the identifier.
   *
   * @return the number of levels, {@code 0} for the root namespace.
   */
  public int levels() {
    return levels.size();
  }

  /**
   * Returns the level at the given position.
   *
   * @param index the zero-based position of the level.
   * @return the level name at the given position.
   */
  public String levelAtListPos(int index) {
    return levels.get(index);
  }

  /**
   * Returns the identifier levels as an immutable list.
   *
   * @return the identifier levels, from the outermost to the innermost.
   */
  public List<String> listStyleId() {
    return Collections.unmodifiableList(levels);
  }
}
