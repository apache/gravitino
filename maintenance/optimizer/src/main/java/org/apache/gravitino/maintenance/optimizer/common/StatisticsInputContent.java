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

package org.apache.gravitino.maintenance.optimizer.common;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

/** Runtime statistics input for calculator implementations that read local payload/file content. */
public final class StatisticsInputContent implements OptimizerContent {
  private final String filePath;
  private final String payload;

  private StatisticsInputContent(String filePath, String payload) {
    this.filePath = filePath;
    this.payload = payload;
  }

  public static StatisticsInputContent fromFilePath(String filePath) {
    Preconditions.checkArgument(StringUtils.isNotBlank(filePath), "filePath must not be blank");
    return new StatisticsInputContent(filePath, null);
  }

  public static StatisticsInputContent fromPayload(String payload) {
    Preconditions.checkArgument(StringUtils.isNotBlank(payload), "payload must not be blank");
    return new StatisticsInputContent(null, payload);
  }

  public boolean hasFilePath() {
    return StringUtils.isNotBlank(filePath);
  }

  public String filePath() {
    return filePath;
  }

  public boolean hasPayload() {
    return StringUtils.isNotBlank(payload);
  }

  public String payload() {
    return payload;
  }
}
