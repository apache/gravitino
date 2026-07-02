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
package org.apache.gravitino.lance.common.ops.hive;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Parses the delimited identifier strings used by the Lance REST surface into the {@code
 * List<String>} form expected by the Lance Namespace request model.
 */
final class IdentifierUtil {

  private IdentifierUtil() {}

  /**
   * Split a delimited identifier into its levels.
   *
   * @param id the identifier, e.g. {@code "db.table"}; {@code null} or empty yields an empty list
   * @param delimiter the literal delimiter, e.g. {@code "."} (treated literally, not as a regex)
   * @return the parsed, non-empty levels in order
   */
  static List<String> parse(String id, String delimiter) {
    if (StringUtils.isEmpty(id)) {
      return Collections.emptyList();
    }
    return Arrays.stream(id.split(Pattern.quote(delimiter)))
        .filter(StringUtils::isNotEmpty)
        .collect(Collectors.toList());
  }
}
