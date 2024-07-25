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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.file.FilesetDTO;

/** Response for fileset creation. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class FilesetResponse extends BaseResponse {

  @JsonProperty("fileset")
  private final FilesetDTO fileset;

  /** Constructor for FilesetResponse. */
  public FilesetResponse() {
    super(0);
    this.fileset = null;
  }

  /**
   * Constructor for FilesetResponse.
   *
   * @param fileset the fileset DTO object.
   */
  public FilesetResponse(FilesetDTO fileset) {
    super(0);
    this.fileset = fileset;
  }

  /**
   * Validates the response.
   *
   * @throws IllegalArgumentException if the response is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(fileset != null, "fileset must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileset.name()), "fileset 'name' must not be null and empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileset.storageLocation()),
        "fileset 'storageLocation' must not be null and empty");
    Preconditions.checkNotNull(fileset.type(), "fileset 'type' must not be null and empty");
  }
}
