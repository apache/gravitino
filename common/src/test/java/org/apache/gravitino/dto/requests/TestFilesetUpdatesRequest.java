/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.dto.requests;

import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilesetUpdatesRequest {

  @Test
  public void testValidateWithNullUpdates() {
    // Test that creating a request with null updates throws an exception during validation
    FilesetUpdatesRequest request = new FilesetUpdatesRequest(null);

    final IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);

    Assertions.assertEquals("Updates list cannot be null", exception.getMessage());
  }

  @Test
  public void testValidateWithEmptyUpdates() {
    // Test that a non-null but empty list of updates is valid
    FilesetUpdatesRequest request = new FilesetUpdatesRequest(Collections.emptyList());

    Assertions.assertDoesNotThrow(request::validate);
  }
}
