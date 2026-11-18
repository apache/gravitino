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
package org.apache.gravitino.utils;

import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSafeUri {

  @Test
  public void testDropsUserInfo() throws Exception {
    String redacted = SafeUri.redact(new URI("ftp://user:s3cr3t@host:21/keytab"));
    Assertions.assertEquals("ftp://host:21/keytab", redacted);
    Assertions.assertFalse(redacted.contains("s3cr3t"));
    Assertions.assertFalse(redacted.contains("user"));
  }

  @Test
  public void testDropsQueryToken() throws Exception {
    String redacted = SafeUri.redact(new URI("https://host/path/file.jar?token=SECRET&x=1"));
    Assertions.assertEquals("https://host/path/file.jar", redacted);
    Assertions.assertFalse(redacted.contains("SECRET"));
  }

  @Test
  public void testKeepsSchemeHostPathForHostlessUri() throws Exception {
    Assertions.assertEquals("file:///tmp/x", SafeUri.redact(new URI("file:///tmp/x")));
  }

  @Test
  public void testNullUri() {
    Assertions.assertEquals("null", SafeUri.redact(null));
  }
}
