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
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.domain.DomainBackingTagDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Creates a draft domain, optionally under a parent, with an explicit backing tag. */
@Getter
public class DomainCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("comment")
  @Nullable
  private final String comment;

  @JsonProperty("properties")
  @Nullable
  private final Map<String, String> properties;

  @JsonProperty("parent")
  @Nullable
  private final String parent;

  @JsonProperty("backingTag")
  private final BackingTag backingTag;

  /** Creates a request for Jackson deserialization. */
  public DomainCreateRequest() {
    this(null, null, null, null, null);
  }

  /**
   * Creates a domain creation request.
   *
   * @param name The new domain name.
   * @param comment The optional comment.
   * @param properties The optional properties.
   * @param parent The optional parent domain name.
   * @param backingTag The explicit backing tag.
   */
  public DomainCreateRequest(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      @Nullable String parent,
      BackingTag backingTag) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.parent = parent;
    this.backingTag = backingTag;
  }

  /** Validates the domain name, optional parent, and explicit backing tag. */
  @Override
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
    Preconditions.checkArgument(
        parent == null || StringUtils.isNotBlank(parent), "parent must not be blank");
    Preconditions.checkArgument(backingTag != null, "backingTag must not be null");
    if (properties != null) {
      properties.forEach(
          (key, value) ->
              Preconditions.checkArgument(
                  key != null && value != null, "properties must not contain null keys or values"));
    }
    backingTag.validate();
  }

  /** A tag to create without a value, or an existing tag to bind. */
  @Getter
  public static class BackingTag extends DomainBackingTagDTO {

    @JsonProperty("create")
    private final boolean create;

    /** Creates a backing tag request for Jackson deserialization. */
    public BackingTag() {
      this(null, null, false);
    }

    /**
     * Creates a backing tag request.
     *
     * @param name The tag name.
     * @param value The optional existing assignment value.
     * @param create Whether to create a new tag without values.
     */
    public BackingTag(String name, @Nullable String value, boolean create) {
      super(name, value);
      this.create = create;
    }

    /** Validates the tag and prohibits creating a tag with a value. */
    @Override
    public void validate() {
      super.validate();
      Preconditions.checkArgument(
          !create || getValue() == null, "A new backing tag cannot have a value");
    }
  }
}
