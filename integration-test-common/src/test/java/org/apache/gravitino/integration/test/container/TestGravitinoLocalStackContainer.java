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
package org.apache.gravitino.integration.test.container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

/**
 * Tests the LocalStack image handling without Docker. Building the container itself needs Docker,
 * as the module asks it for its socket path.
 */
public class TestGravitinoLocalStackContainer {

  @Test
  void testImagesStandInForTheModuleImage() {
    DockerImageName module = DockerImageName.parse("localstack/localstack");
    for (String image :
        new String[] {
          TestImages.LOCALSTACK, "mirror.example.com/localstack/localstack:4.14.0", "localstack:4"
        }) {
      DockerImageName name = GravitinoLocalStackContainer.imageName(image);

      Assertions.assertTrue(name.isCompatibleWith(module), image);
      Assertions.assertEquals(
          DockerImageName.parse(image).asCanonicalNameString(), name.asCanonicalNameString());
    }
  }
}
