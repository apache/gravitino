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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.json.JsonUtils.StrictStringDeserializer;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to associate terms. */
@EqualsAndHashCode
@ToString
public class TermsAssociateRequest implements RESTRequest {

  @JsonProperty("termsToAdd")
  @JsonDeserialize(contentUsing = StrictStringDeserializer.class)
  @Nullable
  private final String[] termsToAdd;

  @JsonProperty("termsToRemove")
  @JsonDeserialize(contentUsing = StrictStringDeserializer.class)
  @Nullable
  private final String[] termsToRemove;

  /**
   * Creates a new TermsAssociateRequest.
   *
   * @param termsToAdd The terms to add.
   * @param termsToRemove The terms to remove.
   */
  public TermsAssociateRequest(@Nullable String[] termsToAdd, @Nullable String[] termsToRemove) {
    this.termsToAdd = termsToAdd == null ? null : termsToAdd.clone();
    this.termsToRemove = termsToRemove == null ? null : termsToRemove.clone();
  }

  /** This is the constructor that is used by Jackson deserializer */
  public TermsAssociateRequest() {
    this(null, null);
  }

  /** Returns the supplied term names to bind, if present. */
  @Nullable
  public String[] getTermsToAdd() {
    return termsToAdd == null ? null : termsToAdd.clone();
  }

  /** Returns the supplied term names to unbind, if present. */
  @Nullable
  public String[] getTermsToRemove() {
    return termsToRemove == null ? null : termsToRemove.clone();
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        termsToAdd != null || termsToRemove != null,
        "termsToAdd and termsToRemove cannot both be null");

    if (termsToAdd != null) {
      for (String term : termsToAdd) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(term), "termsToAdd must not contain null or empty term names");
      }
    }

    if (termsToRemove != null) {
      for (String term : termsToRemove) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(term),
            "termsToRemove must not contain null or empty term names");
      }
    }
  }
}
