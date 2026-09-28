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
package org.apache.gravitino.dto.policy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.policy.TagValueSelector;

/** Data transfer object for a policy-to-tag selector. */
@Getter
@ToString
@EqualsAndHashCode
public class PolicyAssociationSelectorDTO {

  private static final int MAX_VALUE_LENGTH = 256;

  @JsonProperty("type")
  private String type;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("value")
  @Nullable
  private String value;

  private PolicyAssociationSelectorDTO() {}

  /**
   * Creates a selector DTO.
   *
   * @param type The selector type.
   * @param value The selector value, or null for {@link AllValuesSelector}.
   */
  public PolicyAssociationSelectorDTO(String type, @Nullable String value) {
    this.type = type;
    this.value = value;
  }

  /**
   * Creates a DTO from an API selector.
   *
   * @param selector The API selector.
   * @return The selector DTO.
   */
  public static PolicyAssociationSelectorDTO fromSelector(PolicyAssociationSelector selector) {
    Preconditions.checkArgument(selector != null, "Selector cannot be null");
    if (selector instanceof AllValuesSelector) {
      return new PolicyAssociationSelectorDTO(AllValuesSelector.TYPE, null);
    }
    if (selector instanceof TagValueSelector) {
      return new PolicyAssociationSelectorDTO(
          TagValueSelector.TYPE, ((TagValueSelector) selector).value());
    }
    throw new IllegalArgumentException("Unsupported selector type: " + selector.type());
  }

  /** Validates this selector DTO. */
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(type), "Selector type cannot be blank");
    if (AllValuesSelector.TYPE.equals(type)) {
      Preconditions.checkArgument(value == null, "ALL_VALUES selector must not define a value");
      return;
    }
    Preconditions.checkArgument(
        TagValueSelector.TYPE.equals(type), "Unsupported selector type: %s", type);
    Preconditions.checkArgument(StringUtils.isNotBlank(value), "Selector value cannot be blank");
    Preconditions.checkArgument(
        value.length() <= MAX_VALUE_LENGTH,
        "Selector value must not exceed %s characters",
        MAX_VALUE_LENGTH);
  }

  /**
   * Converts this DTO to an API selector.
   *
   * @return The API selector.
   */
  public PolicyAssociationSelector toSelector() {
    validate();
    if (AllValuesSelector.TYPE.equals(type)) {
      return AllValuesSelector.get();
    }
    return TagValueSelector.of(value);
  }
}
