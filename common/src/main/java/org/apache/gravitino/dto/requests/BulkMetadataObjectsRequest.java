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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.rest.RESTRequest;

/** A request to load effective tags or policies for a list of metadata objects. */
@Getter
public class BulkMetadataObjectsRequest implements RESTRequest {

  @JsonProperty("metadataObjects")
  private final MetadataObjectDTO[] metadataObjects;

  /**
   * Creates a bulk metadata object lookup request.
   *
   * @param metadataObjects the objects whose associations should be loaded
   */
  public BulkMetadataObjectsRequest(MetadataObjectDTO[] metadataObjects) {
    this.metadataObjects = metadataObjects;
  }

  /** Creates a request for Jackson deserialization. */
  public BulkMetadataObjectsRequest() {
    this(null);
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        metadataObjects != null && metadataObjects.length > 0, "metadataObjects must not be empty");
    Set<MetadataObject> seen = new HashSet<>();
    for (MetadataObjectDTO object : metadataObjects) {
      Preconditions.checkArgument(
          object != null && MetadataObjects.TAGGABLE_METADATA_OBJECT_TYPES.contains(object.type()),
          "metadataObject must have a supported type");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(object.name()), "metadataObject fullName must not be empty");
      MetadataObject parsed = MetadataObjects.parse(object.fullName(), object.type());
      Preconditions.checkArgument(seen.add(parsed), "Duplicate metadata object: %s", parsed);
    }
  }
}
