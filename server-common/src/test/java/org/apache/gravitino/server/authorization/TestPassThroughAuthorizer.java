/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

import java.io.IOException;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test of {@link PassThroughAuthorizer} */
public class TestPassThroughAuthorizer {

  @Test
  public void testAuthorize() throws IOException {
    try (PassThroughAuthorizer passThroughAuthorizer = new PassThroughAuthorizer()) {
      boolean result =
          passThroughAuthorizer.authorize(
              null, null, null, null, new AuthorizationRequestContext());
      Assertions.assertTrue(result, "Logic error in PassThroughAuthorizer");
    }
  }
}
