/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
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
 *  under the License.
 */

package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Response for the actual file location. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class FileLocationResponse extends BaseResponse {
  @JsonProperty("fileLocation")
  private final String fileLocation;

  /** Constructor for FileLocationResponse. */
  public FileLocationResponse() {
    super(0);
    this.fileLocation = null;
  }

  /**
   * Constructor for FileLocationResponse.
   *
   * @param fileLocation the actual file location.
   */
  public FileLocationResponse(String fileLocation) {
    super(0);
    this.fileLocation = fileLocation;
  }

  /**
   * Validates the response.
   *
   * @throws IllegalArgumentException if the response is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileLocation), "fileLocation must not be null");
  }
}
