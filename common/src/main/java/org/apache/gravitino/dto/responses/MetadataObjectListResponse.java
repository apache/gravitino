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
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;

/** Represents a response containing a list of metadata objects. */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class MetadataObjectListResponse extends BaseResponse {

  @JsonProperty("metadataObjects")
  private final MetadataObjectDTO[] metadataObjects;

  /**
   * Constructor for MetadataObjectListResponse.
   *
   * @param metadataObjects The array of metadata object DTOs.
   */
  public MetadataObjectListResponse(MetadataObjectDTO[] metadataObjects) {
    super(0);
    this.metadataObjects = metadataObjects;
  }

  /** Default constructor for MetadataObjectListResponse. (Used for Jackson deserialization.) */
  public MetadataObjectListResponse() {
    super();
    this.metadataObjects = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if name or audit information is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(metadataObjects != null, "metadataObjects must be non-null");
    Arrays.stream(metadataObjects)
        .forEach(
            object ->
                Preconditions.checkArgument(
                    object != null
                        && StringUtils.isNotBlank(object.name())
                        && object.type() != null,
                    "metadataObject must not be null and it's field cannot null or empty"));
  }
}
