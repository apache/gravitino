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
package org.apache.gravitino.connector.capability;

import org.apache.gravitino.MetadataObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCapability {

  @Test
  void testDefaultNameSpecification() {
    for (Capability.Scope scope : Capability.Scope.values()) {
      // test for normal name
      CapabilityResult result = Capability.DEFAULT.specificationOnName(scope, "_name_123_/_=-");
      Assertions.assertTrue(result.supported());

      result = Capability.DEFAULT.specificationOnName(scope, "name_123_/_=-");
      Assertions.assertTrue(result.supported());

      result = Capability.DEFAULT.specificationOnName(scope, "Name_123_/_=-");
      Assertions.assertTrue(result.supported());

      // test for reserved name
      result =
          Capability.DEFAULT.specificationOnName(
              scope, MetadataObjects.METADATA_OBJECT_RESERVED_NAME);
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is reserved"));

      // test for illegal name
      result = Capability.DEFAULT.specificationOnName(scope, "name with space");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "name_with_@");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "name_with_#");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "name_with_$");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "name_with_%");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "-name_start_with-");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "/name_start_with/");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      result = Capability.DEFAULT.specificationOnName(scope, "=name_start_with=");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));

      // test for long name
      StringBuilder longName = new StringBuilder();
      for (int i = 0; i < 64; i++) {
        longName.append("a");
      }

      Assertions.assertEquals(64, longName.length());
      result = Capability.DEFAULT.specificationOnName(scope, longName.toString());
      Assertions.assertTrue(result.supported());

      longName.append("a");
      Assertions.assertEquals(65, longName.length());
      result = Capability.DEFAULT.specificationOnName(scope, longName.toString());
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("is illegal"));
    }
  }
}
