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
package org.apache.gravitino.credential;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.dto.credential.CredentialDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCredentialDTO {

  @Test
  public void testCredentialDTOSerDe() throws JsonProcessingException {
    Map<String, String> credentialInfo =
        ImmutableMap.of(
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID, "access-key",
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY, "secret-key");

    CredentialDTO credentialDTO =
        CredentialDTO.builder()
            .withCredentialType(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE)
            .withCredentialInfo(credentialInfo)
            .withExpireTimeInMs(10)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(credentialDTO);
    CredentialDTO deserCredentialDTO =
        JsonUtils.objectMapper().readValue(serJson, CredentialDTO.class);
    Assertions.assertEquals(credentialDTO, deserCredentialDTO);
  }
}
