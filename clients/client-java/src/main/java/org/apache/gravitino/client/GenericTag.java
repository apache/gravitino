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
package org.apache.gravitino.client;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.responses.MetadataObjectListResponse;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.tag.Tag;

/** Represents a generic tag. */
class GenericTag implements Tag, Tag.AssociatedObjects {

  private final TagDTO tagDTO;

  private final RESTClient restClient;

  private final String metalake;

  GenericTag(TagDTO tagDTO, RESTClient restClient, String metalake) {
    this.tagDTO = tagDTO;
    this.restClient = restClient;
    this.metalake = metalake;
  }

  @Override
  public String name() {
    return tagDTO.name();
  }

  @Override
  public String comment() {
    return tagDTO.comment();
  }

  @Override
  public Map<String, String> properties() {
    return tagDTO.properties();
  }

  @Override
  public Optional<Boolean> inherited() {
    return tagDTO.inherited();
  }

  @Override
  public Audit auditInfo() {
    return tagDTO.auditInfo();
  }

  @Override
  public AssociatedObjects associatedObjects() {
    return this;
  }

  @Override
  public MetadataObject[] objects() {
    MetadataObjectListResponse resp =
        restClient.get(
            String.format(
                "api/metalakes/%s/tags/%s/objects",
                RESTUtils.encodeString(metalake), RESTUtils.encodeString(name())),
            MetadataObjectListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());

    resp.validate();
    return resp.getMetadataObjects();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GenericTag)) {
      return false;
    }

    GenericTag that = (GenericTag) obj;
    return tagDTO.equals(that.tagDTO);
  }

  @Override
  public int hashCode() {
    return tagDTO.hashCode();
  }
}
