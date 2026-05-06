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
package org.apache.gravitino.catalog.glue.integration.test;

import java.util.Map;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.integration.test.container.GravitinoMotoContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

/**
 * Runs {@link AbstractGlueCatalogIT} scenarios against a Moto server (free AWS mock).
 *
 * <p>Requires Docker. Skipped by default when the {@code gravitino-docker-test} tag is excluded.
 * Override the container image with the {@code GRAVITINO_CI_MOTO_DOCKER_IMAGE} environment
 * variable.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MotoGlueCatalogIT extends AbstractGlueCatalogIT {

  private static final String FALLBACK_IMAGE = "motoserver/moto:5.1.4";

  private GravitinoMotoContainer moto;

  @Override
  @BeforeAll
  void initOps() {
    String image = GravitinoMotoContainer.DEFAULT_IMAGE;
    if (image == null || image.isBlank()) {
      image = FALLBACK_IMAGE;
    }
    moto = GravitinoMotoContainer.builder().withImage(image).build();
    moto.start();
    super.initOps();
  }

  @AfterAll
  void stopContainer() {
    if (moto != null) {
      moto.close();
    }
  }

  @Override
  protected Map<String, String> catalogConfig() {
    String endpoint =
        "http://"
            + moto.getContainer().getHost()
            + ":"
            + moto.getMappedPort(GravitinoMotoContainer.PORT);
    return Map.of(
        GlueConstants.AWS_REGION,
        "us-east-1",
        GlueConstants.AWS_ACCESS_KEY_ID,
        "test",
        GlueConstants.AWS_SECRET_ACCESS_KEY,
        "test",
        GlueConstants.AWS_GLUE_ENDPOINT,
        endpoint);
  }
}
