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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.TagListResponse;
import org.apache.gravitino.dto.responses.TagResponse;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/**
 * The implementation of {@link SupportsTags}. This interface will be composited into catalog,
 * schema, table, fileset and topic to provide tag operations for these metadata objects
 */
class MetadataObjectTagOperations implements SupportsTags {

  private final String metalakeName;

  private final RESTClient restClient;

  private final String tagRequestPath;

  MetadataObjectTagOperations(
      String metalakeName, MetadataObject metadataObject, RESTClient restClient) {
    this.metalakeName = metalakeName;
    this.restClient = restClient;
    this.tagRequestPath =
        String.format(
            "api/metalakes/%s/objects/%s/%s/tags",
            RESTUtils.encodeString(metalakeName),
            metadataObject.type().name().toLowerCase(Locale.ROOT),
            RESTUtils.encodeString(metadataObject.fullName()));
  }

  @Override
  public String[] listTags() {
    NameListResponse resp =
        restClient.get(
            tagRequestPath,
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());

    resp.validate();
    return resp.getNames();
  }

  @Override
  public Tag[] listTagsInfo() {
    TagListResponse resp =
        restClient.get(
            tagRequestPath,
            ImmutableMap.of("details", "true"),
            TagListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());

    resp.validate();
    return Arrays.stream(resp.getTags())
        .map(tagDTO -> new GenericTag(tagDTO, restClient, metalakeName))
        .toArray(Tag[]::new);
  }

  @Override
  public Tag getTag(String name) throws NoSuchTagException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "Tag name must not be null or empty");

    TagResponse resp =
        restClient.get(
            tagRequestPath + "/" + RESTUtils.encodeString(name),
            TagResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());

    resp.validate();
    return new GenericTag(resp.getTag(), restClient, metalakeName);
  }

  @Override
  public String[] associateTags(String[] tagsToAdd, String[] tagsToRemove) {
    TagsAssociateRequest request = new TagsAssociateRequest(tagsToAdd, tagsToRemove);
    request.validate();

    NameListResponse resp =
        restClient.post(
            tagRequestPath,
            request,
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tagErrorHandler());

    resp.validate();
    return resp.getNames();
  }
}
