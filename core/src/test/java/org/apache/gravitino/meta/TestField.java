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
package org.apache.gravitino.meta;

import org.apache.gravitino.Field;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestField {

  @Test
  public void testFieldRequired() {
    Field required = Field.required("test", String.class, "test");
    required.validate("test");
    Throwable exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> required.validate(null));
    Assertions.assertEquals("Field test is required", exception.getMessage());
  }

  @Test
  public void testFieldOptional() {
    Field optional = Field.optional("test", String.class, "test");
    optional.validate("test");
    optional.validate(null);
  }

  @Test
  public void testTypeUnmatched() {
    Field required = Field.required("test", String.class, "test");
    Throwable exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> required.validate(1));
    Assertions.assertEquals("Field test is not of type java.lang.String", exception.getMessage());

    Field optional = Field.optional("test1", Integer.class, "test");
    Throwable exception1 =
        Assertions.assertThrows(IllegalArgumentException.class, () -> optional.validate(1L));
    Assertions.assertEquals(
        "Field test1 is not of type java.lang.Integer", exception1.getMessage());
  }
}
