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

package org.apache.gravitino.job.local;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.job.HttpJobTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHttpProcessBuilder {

  @Test
  public void testGenerateHttpCommand() {
    HttpJobTemplate template1 =
        HttpJobTemplate.builder()
            .withName("template1")
            .withExecutable("GET")
            .withUrl("http://example.com/api")
            .withArguments(Lists.newArrayList("arg1", "arg2"))
            .build();

    List<String> command = HttpProcessBuilder.generateHttpCommand(template1);

    Assertions.assertEquals("curl", command.get(0));
    Assertions.assertEquals("-X", command.get(1));
    Assertions.assertEquals("GET", command.get(2));
    Assertions.assertEquals("http://example.com/api", command.get(3));
    Assertions.assertTrue(command.contains("arg1"));
    Assertions.assertTrue(command.contains("arg2"));
  }

  @Test
  public void testGenerateHttpCommandWithHeaders() {
    HttpJobTemplate template =
        HttpJobTemplate.builder()
            .withName("template2")
            .withExecutable("POST")
            .withUrl("http://example.com/api")
            .withHeaders(
                ImmutableMap.of(
                    "Content-Type", "application/json", "Authorization", "Bearer token"))
            .withBody("{\"key\": \"value\"}")
            .build();

    List<String> command = HttpProcessBuilder.generateHttpCommand(template);

    Assertions.assertEquals("curl", command.get(0));
    Assertions.assertEquals("-X", command.get(1));
    Assertions.assertEquals("POST", command.get(2));

    // Check headers
    Assertions.assertTrue(command.contains("-H"));
    int contentTypeIndex = command.indexOf("-H") + 1;
    Assertions.assertEquals("Content-Type: application/json", command.get(contentTypeIndex));

    int authIndex = command.lastIndexOf("-H") + 1;
    Assertions.assertEquals("Authorization: Bearer token", command.get(authIndex));

    // Check body
    Assertions.assertTrue(command.contains("-d"));
    int bodyIndex = command.indexOf("-d") + 1;
    Assertions.assertEquals("{\"key\": \"value\"}", command.get(bodyIndex));

    // Check URL
    Assertions.assertEquals("http://example.com/api", command.get(command.size() - 1));
  }

  @Test
  public void testGenerateHttpCommandWithQueryParams() {
    HttpJobTemplate template =
        HttpJobTemplate.builder()
            .withName("template3")
            .withExecutable("PUT")
            .withUrl("http://example.com/api")
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .build();

    List<String> command = HttpProcessBuilder.generateHttpCommand(template);

    Assertions.assertEquals("curl", command.get(0));
    Assertions.assertEquals("-X", command.get(1));
    Assertions.assertEquals("PUT", command.get(2));
    Assertions.assertEquals("http://example.com/api?param1=value1&param2=value2", command.get(3));
  }

  @Test
  public void testGenerateHttpCommandWithAllOptions() {
    HttpJobTemplate template =
        HttpJobTemplate.builder()
            .withName("template4")
            .withExecutable("DELETE")
            .withUrl("http://example.com/api")
            .withHeaders(ImmutableMap.of("Content-Type", "application/json"))
            .withBody("{\"key\": \"value\"}")
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .withArguments(Lists.newArrayList("arg1", "arg2"))
            .build();

    List<String> command = HttpProcessBuilder.generateHttpCommand(template);

    Assertions.assertEquals("curl", command.get(0));
    Assertions.assertEquals("-X", command.get(1));
    Assertions.assertEquals("DELETE", command.get(2));

    // Check headers
    Assertions.assertTrue(command.contains("-H"));
    int headerIndex = command.indexOf("-H") + 1;
    Assertions.assertEquals("Content-Type: application/json", command.get(headerIndex));

    // Check body
    Assertions.assertTrue(command.contains("-d"));
    int bodyIndex = command.indexOf("-d") + 1;
    Assertions.assertEquals("{\"key\": \"value\"}", command.get(bodyIndex));

    // Check URL with query params
    Assertions.assertEquals(
        "http://example.com/api?param1=value1&param2=value2", command.get(command.size() - 3));

    // Check additional arguments
    Assertions.assertTrue(command.contains("arg1"));
    Assertions.assertTrue(command.contains("arg2"));
  }
}
