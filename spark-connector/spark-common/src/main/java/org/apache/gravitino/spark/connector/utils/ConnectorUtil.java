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

package org.apache.gravitino.spark.connector.utils;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.spark.connector.ConnectorConstants;

public class ConnectorUtil {

  public static String removeDuplicateSparkExtensions(
      String[] extensions, String[] addedExtensions) {
    Set<String> uniqueElements = new LinkedHashSet<>(Arrays.asList(extensions));
    if (addedExtensions != null && StringUtils.isNoneBlank(addedExtensions)) {
      uniqueElements.addAll(Arrays.asList(addedExtensions));
    }
    return uniqueElements.stream()
        .reduce((element1, element2) -> element1 + ConnectorConstants.COMMA + element2)
        .orElse("");
  }
}
