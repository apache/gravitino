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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.dto.requests.PolicyTagAddRequest;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.TagValueSelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyAssociationSelectorDTO {

  @Test
  public void testAllValuesSelectorSerDe() throws JsonProcessingException {
    PolicyAssociationSelectorDTO selector =
        PolicyAssociationSelectorDTO.fromSelector(AllValuesSelector.get());

    String json = JsonUtils.objectMapper().writeValueAsString(selector);
    PolicyAssociationSelectorDTO deserialized =
        JsonUtils.objectMapper().readValue(json, PolicyAssociationSelectorDTO.class);

    Assertions.assertEquals(selector, deserialized);
    Assertions.assertFalse(json.contains("value"));
    Assertions.assertSame(AllValuesSelector.get(), deserialized.toSelector());
  }

  @Test
  public void testTagValueSelectorRequestSerDe() throws JsonProcessingException {
    PolicyAssociationSelectorDTO selector =
        PolicyAssociationSelectorDTO.fromSelector(TagValueSelector.of("finance"));
    PolicyTagAddRequest request = new PolicyTagAddRequest(selector);

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    PolicyTagAddRequest deserialized =
        JsonUtils.objectMapper().readValue(json, PolicyTagAddRequest.class);

    deserialized.validate();
    Assertions.assertEquals(TagValueSelector.of("finance"), deserialized.selector());
  }

  @Test
  public void testInvalidSelectors() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> PolicyAssociationSelectorDTO.fromSelector(null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new PolicyAssociationSelectorDTO("UNKNOWN", null).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new PolicyAssociationSelectorDTO(AllValuesSelector.TYPE, "unexpected").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new PolicyAssociationSelectorDTO(TagValueSelector.TYPE, " ").validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new PolicyAssociationSelectorDTO(TagValueSelector.TYPE, "a".repeat(257)).validate());
    Assertions.assertThrows(IllegalArgumentException.class, new PolicyTagAddRequest()::validate);
  }
}
