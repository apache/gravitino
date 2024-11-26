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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TableFormatOutputIT extends BaseIT {
  private String gravitinoUrl;

  @BeforeAll
  public void startUp() {
    gravitinoUrl = String.format("http://127.0.0.1:%d", getGravitinoServerPort());
  }

  @Test
  public void testMetalakeListCommand() {
    // Create a byte array output stream to capture the output of the command
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outputStream));

    String[] args = {
      "metalake",
      "list",
      commandArg(GravitinoOptions.OUTPUT),
      "table",
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(args);

    // Restore the original System.out
    System.setOut(originalOut);
    // Get the output and verify it
    String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals("+----------+\n" + "| metalake |\n" + "+----------+\n" + "+----------+", output);
  }

  private String commandArg(String arg) {
    return String.format("--%s", arg);
  }
}
