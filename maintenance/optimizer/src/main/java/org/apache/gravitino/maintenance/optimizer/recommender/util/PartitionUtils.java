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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.util.PartitionPathSerdeUtils;

/** Helpers for converting between Gravitino partition names and {@link PartitionPath}. */
public class PartitionUtils {
  private PartitionUtils() {}

  /**
   * Encodes a {@link PartitionPath} into a JSON string.
   *
   * <p>For example, a path with entries {@code p1=v1, p2=v2} is encoded as {@code [{"p1":"v1"},
   * {"p2":"v2"}]}.
   *
   * @param partitionPath partition path
   * @return encoded JSON string
   */
  public static String encodePartitionPath(PartitionPath partitionPath) {
    return PartitionPathSerdeUtils.encode(partitionPath);
  }

  /**
   * Decodes a JSON-encoded partition path into a {@link PartitionPath}.
   *
   * <p>Example format: {@code [{"p1":"v1"},{"p2":"v2"}]}.
   *
   * @param encodedPartitionPath JSON string representing the partition path
   * @return parsed partition path
   */
  public static PartitionPath decodePartitionPath(String encodedPartitionPath) {
    return PartitionPathSerdeUtils.decode(encodedPartitionPath);
  }
}
