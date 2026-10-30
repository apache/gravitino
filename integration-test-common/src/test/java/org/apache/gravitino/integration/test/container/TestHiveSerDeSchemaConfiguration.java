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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

class TestHiveSerDeSchemaConfiguration {
  @TempDir Path configurationDirectory;

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void testPrebuiltImageConfigurationPreservesExistingProperties() throws Exception {
    String script;
    try (InputStream source =
        getClass().getResourceAsStream("/hive/enable-serde-schema-tests.sh")) {
      Assertions.assertNotNull(source);
      script = new String(source.readAllBytes(), StandardCharsets.UTF_8);
    }
    // Run the actual configuration commands without starting the image's services.
    script = script.substring(0, script.indexOf("exec /bin/bash"));
    List<String> files = List.of("hive-site.xml", "hive-site-for-sql-base-auth.xml");
    for (String file : files) {
      Files.writeString(
          configurationDirectory.resolve(file),
          "<configuration>\n"
              + "<property><name>existing</name><value>keep</value></property>\n"
              + "<property><name>hive.metastore.disallow.incompatible.col.type.changes</name><value>true</value></property>\n"
              + "</configuration>\n");
    }
    ProcessBuilder builder = new ProcessBuilder("bash", "-c", script).redirectErrorStream(true);
    builder.environment().put("HIVE_TMP_CONF_DIR", configurationDirectory.toString());
    Process process = builder.start();
    try {
      Assertions.assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      try (InputStream output = process.getInputStream()) {
        Assertions.assertEquals(
            0, process.exitValue(), new String(output.readAllBytes(), StandardCharsets.UTF_8));
      }
    } finally {
      process.destroyForcibly();
    }
    for (String file : files) {
      NodeList properties =
          DocumentBuilderFactory.newInstance()
              .newDocumentBuilder()
              .parse(configurationDirectory.resolve(file).toFile())
              .getElementsByTagName("property");
      Map<String, String> values = new HashMap<>();
      for (int index = 0; index < properties.getLength(); index++) {
        Element property = (Element) properties.item(index);
        values.put(
            property.getElementsByTagName("name").item(0).getTextContent(),
            property.getElementsByTagName("value").item(0).getTextContent());
      }
      Assertions.assertEquals("keep", values.get("existing"));
      Assertions.assertEquals(
          "false", values.get("hive.metastore.disallow.incompatible.col.type.changes"));
      Assertions.assertEquals(
          "org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader",
          values.get("metastore.storage.schema.reader.impl"));
    }
  }
}
