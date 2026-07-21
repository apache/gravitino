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
package org.apache.gravitino.kms.transit.integration.test;

import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.kms.transit.OpenBaoTransitKmsClientFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenBaoTransitKmsClientIT extends AbstractTransitKmsClientIT {

  private static final String DEFAULT_IMAGE = "openbao/openbao:2.6.0";
  private static final String IMAGE_ENVIRONMENT_VARIABLE = "GRAVITINO_OPENBAO_DOCKER_IMAGE";

  @Override
  protected String image() {
    String configuredImage = System.getenv(IMAGE_ENVIRONMENT_VARIABLE);
    return configuredImage == null || configuredImage.trim().isEmpty()
        ? DEFAULT_IMAGE
        : configuredImage.trim();
  }

  @Override
  protected String executable() {
    return "bao";
  }

  @Override
  protected String addressEnvironmentVariable() {
    return "BAO_ADDR";
  }

  @Override
  protected String tokenEnvironmentVariable() {
    return "BAO_TOKEN";
  }

  @Override
  protected KmsApi api() {
    return KmsApi.OPENBAO_TRANSIT;
  }

  @Override
  protected KmsClientFactory factory() {
    return new OpenBaoTransitKmsClientFactory();
  }
}
