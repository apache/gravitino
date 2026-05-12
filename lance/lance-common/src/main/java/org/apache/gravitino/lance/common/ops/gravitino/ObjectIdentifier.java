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

/** Utility identifier parser for Lance namespace/table string IDs. */
class ObjectIdentifier {

  private final List<String> levels;

  private ObjectIdentifier(List<String> levels) {
    this.levels = levels;
  }

  static ObjectIdentifier of(String id, String delimiterRegex) {
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

  int levels() {
    return levels.size();
  }

  String levelAtListPos(int index) {
    return levels.get(index);
  }

  List<String> listStyleId() {
    return Collections.unmodifiableList(levels);
  }
}
