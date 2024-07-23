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
 * The implementation of {@link SupportsTags}. This interface will be mixed into catalog, schema,
 * table, fileset and topic to provide tag operations for these metadata objects
 */
interface SupportsTagOperations extends SupportsTags {

  /** @return The name of the metalake. */
  String metalakeName();

  /** @return The metadata object. */
  MetadataObject metadataObject();

  /** @return The REST client. */
  RESTClient restClient();

  @Override
  default String[] listTags() {
    NameListResponse resp =
        restClient()
            .get(
                formatTagRequestPath(),
                NameListResponse.class,
                Collections.emptyMap(),
                ErrorHandlers.tagErrorHandler());

    resp.validate();
    return resp.getNames();
  }

  @Override
  default Tag[] listTagsInfo() {
    TagListResponse resp =
        restClient()
            .get(
                formatTagRequestPath(),
                TagListResponse.class,
                Collections.emptyMap(),
                ErrorHandlers.tagErrorHandler());

    resp.validate();
    return Arrays.stream(resp.getTags())
        .map(tagDTO -> new GenericTag(tagDTO, restClient(), metalakeName()))
        .toArray(Tag[]::new);
  }

  @Override
  default Tag getTag(String name) throws NoSuchTagException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "Tag name must not be null or empty");

    TagResponse resp =
        restClient()
            .get(
                formatTagRequestPath() + "/" + RESTUtils.encodeString(name),
                TagResponse.class,
                Collections.emptyMap(),
                ErrorHandlers.tagErrorHandler());

    resp.validate();
    return new GenericTag(resp.getTag(), restClient(), metalakeName());
  }

  @Override
  default String[] associateTags(String[] tagsToAdd, String[] tagsToRemove) {
    TagsAssociateRequest request = new TagsAssociateRequest(tagsToAdd, tagsToRemove);
    request.validate();

    NameListResponse resp =
        restClient()
            .post(
                formatTagRequestPath(),
                request,
                NameListResponse.class,
                Collections.emptyMap(),
                ErrorHandlers.tagErrorHandler());

    resp.validate();
    return resp.getNames();
  }

  /**
   * Formats the request path for tags.
   *
   * @return The formatted request path.
   */
  default String formatTagRequestPath() {
    return String.format(
        "api/metalakes/%s/tags/%s/%s",
        metalakeName(),
        metadataObject().type().name().toLowerCase(Locale.ROOT),
        metadataObject().fullName());
  }
}
