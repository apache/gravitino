/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests that the fixture consumes exactly one versioned, immutable image reference. */
public class TestRustFSContainer {

  @Test
  void testImagePinFromPackagedResource() {
    Assertions.assertTrue(
        RustFSContainer.DEFAULT_IMAGE.matches("rustfs/rustfs:[^@\\s]+@sha256:[0-9a-f]{64}"));
  }

  @Test
  void testImagePinParsing() {
    String manifest = "services:\n  rustfs:\n    image: " + RustFSContainer.DEFAULT_IMAGE + "\n";
    Assertions.assertEquals(
        RustFSContainer.DEFAULT_IMAGE, RustFSContainer.imageFromCompose(manifest));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RustFSContainer.imageFromCompose(manifest + "    image: another/image:latest\n"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RustFSContainer.imageFromCompose("services:\n  rustfs:\n"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RustFSContainer.imageFromCompose("    image: rustfs/rustfs:latest\n"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RustFSContainer.imageFromCompose("    image: rustfs/rustfs:1.0.0@sha256:bad\n"));
  }
}
