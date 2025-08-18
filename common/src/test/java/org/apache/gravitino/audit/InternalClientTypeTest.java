/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
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
 *  under the License.
 */

package org.apache.gravitino.audit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InternalClientTypeTest {

  @Test
  public void testCheckValid() {
    Assertions.assertTrue(InternalClientType.checkValid("HADOOP_GVFS"));
    Assertions.assertTrue(InternalClientType.checkValid("PYTHON_GVFS"));
    Assertions.assertTrue(InternalClientType.checkValid("UNKNOWN"));
    Assertions.assertFalse(InternalClientType.checkValid("NO_SUCH_CLIENT"));
    Assertions.assertFalse(InternalClientType.checkValid(null));
  }

  @Test
  public void testCheckValidWithEmptyString() {
    Assertions.assertFalse(InternalClientType.checkValid(""));
  }

  @Test
  public void testCheckValidWithWhitespace() {
    Assertions.assertFalse(InternalClientType.checkValid(" "));
    Assertions.assertFalse(InternalClientType.checkValid("  HADOOP_GVFS  "));
  }

  @Test
  public void testCheckValidWithCaseSensitivity() {
    Assertions.assertFalse(InternalClientType.checkValid("hadoop_gvfs"));
    Assertions.assertFalse(InternalClientType.checkValid("Hadoop_Gvfs"));
  }
}
