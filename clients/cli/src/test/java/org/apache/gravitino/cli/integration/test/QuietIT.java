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

package org.apache.gravitino.cli.integration.test;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

public class QuietIT extends BaseIT {
  private String gravitinoUrl;

  @BeforeAll
  public void startUp() {
    gravitinoUrl = String.format("http://127.0.0.1:%d", getGravitinoServerPort());
    String[] create_metalake_args = {
      "metalake",
      "create",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.COMMENT),
      "my metalake",
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(create_metalake_args);
  }

  @Test
  void testQuietOption() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outputStream));

    String[] disableArgs = {
      "metalake",
      "update",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.QUIET),
      commandArg(GravitinoOptions.DISABLE),
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };

    Main.main(disableArgs);
    // Restore the original System.out
    System.setOut(originalOut);
    String stdOutput = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
    assertTrue(StringUtils.isBlank(stdOutput));
  }

  private String commandArg(String arg) {
    return String.format("--%s", arg);
  }
}
