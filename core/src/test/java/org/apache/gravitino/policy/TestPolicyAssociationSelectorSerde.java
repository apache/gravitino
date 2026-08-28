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
package org.apache.gravitino.policy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyAssociationSelectorSerde {

  @Test
  void testAllValuesRoundTrip() {
    String json = PolicyAssociationSelectorSerde.serialize(AllValuesSelector.get());

    Assertions.assertEquals("{\"type\":\"ALL_VALUES\"}", json);
    Assertions.assertSame(
        AllValuesSelector.get(), PolicyAssociationSelectorSerde.deserialize(json));
  }

  @Test
  void testTagValueRoundTrip() {
    PolicyAssociationSelector selector = TagValueSelector.of("finance");

    String json = PolicyAssociationSelectorSerde.serialize(selector);

    Assertions.assertEquals("{\"type\":\"TAG_VALUE\",\"value\":\"finance\"}", json);
    Assertions.assertEquals(selector, PolicyAssociationSelectorSerde.deserialize(json));
  }

  @Test
  void testLegacyNullSelectorUsesAllValues() {
    Assertions.assertSame(
        AllValuesSelector.get(), PolicyAssociationSelectorSerde.deserialize(null));
  }

  @Test
  void testRejectUnsupportedSelector() {
    PolicyAssociationSelector selector = () -> "CUSTOM";

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PolicyAssociationSelectorSerde.serialize(selector));

    Assertions.assertTrue(
        exception.getMessage().contains("Unsupported policy association selector"));
  }

  @Test
  void testRejectInvalidJson() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> PolicyAssociationSelectorSerde.deserialize("[]"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PolicyAssociationSelectorSerde.deserialize("{\"type\":\"UNKNOWN\"}"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PolicyAssociationSelectorSerde.deserialize("{\"type\":\"TAG_VALUE\"}"));
  }
}
