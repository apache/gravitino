/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.deletion;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

/** Safe public representation of one Iceberg table deletion action. */
@Getter
@Builder
public class IcebergDeletionAction {
  @JsonProperty("deletionId")
  private final String deletionId;

  @JsonProperty("entityId")
  private final String entityId;

  private final String state;

  @JsonProperty("deletedAt")
  private final long deletedAt;

  @Nullable
  @JsonInclude(JsonInclude.Include.ALWAYS)
  @JsonProperty("retentionExpiresAt")
  private final Long retentionExpiresAt;

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("purgeJobId")
  private final String purgeJobId;

  private final boolean recoverable;
}
