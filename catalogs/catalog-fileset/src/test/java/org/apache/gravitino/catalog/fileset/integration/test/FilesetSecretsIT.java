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
package org.apache.gravitino.catalog.fileset.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * IT covering fileset create-time secretBindings with the in-memory secret provider and getSecrets.
 */
public class FilesetSecretsIT extends BaseIT {

  private String metalakeName = GravitinoITUtils.genRandomName("fileset_secrets_ml");
  private String catalogName = GravitinoITUtils.genRandomName("fileset_secrets_cat");
  private String schemaName = GravitinoITUtils.genRandomName("fileset_secrets_schema");
  private GravitinoMetalake metalake;
  private Catalog catalog;
  private String baseLocation;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    configs.put(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    baseLocation =
        Files.createTempDirectory("fileset-secrets-it").toAbsolutePath().toUri().toString();
    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);
    metalake.createCatalog(
        catalogName, Catalog.Type.FILESET, "hadoop", "comment", ImmutableMap.of());
    catalog = metalake.loadCatalog(catalogName);
    catalog
        .asSchemas()
        .createSchema(schemaName, "comment", ImmutableMap.of("location", baseLocation));
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (metalake != null) {
      catalog.asSchemas().dropSchema(schemaName, true);
      metalake.dropCatalog(catalogName, true);
      client.dropMetalake(metalakeName, true);
    }
    super.stopIntegrationTest();
  }

  @Test
  public void testCreateFilesetWithMemorySecretBindingsAndGetSecrets() {
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, "secret_fileset");
    String location = baseLocation + "/secret_fileset";
    Fileset fileset =
        catalog
            .asFilesetCatalog()
            .createFileset(
                filesetIdent,
                "comment",
                Fileset.Type.MANAGED,
                location,
                ImmutableMap.of("visible-key", "visible-value"),
                ImmutableMap.of("custom-secret", new SecretBinding("memory", "mem-plaintext")),
                ImmutableMap.of());

    Assertions.assertEquals("visible-value", fileset.properties().get("visible-key"));
    Assertions.assertTrue(
        fileset.properties().get("custom-secret").startsWith("gravitino-secret://"),
        "create-time binding should persist as URN, got: "
            + fileset.properties().get("custom-secret"));

    Map<String, String> secrets = fileset.supportsSecrets().getSecrets();
    Assertions.assertEquals("mem-plaintext", secrets.get("custom-secret"));
    Assertions.assertFalse(secrets.containsKey("visible-key"));
  }
}
