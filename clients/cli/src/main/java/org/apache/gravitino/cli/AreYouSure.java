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

package org.apache.gravitino.cli;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/** Ask are you sure you want to do this? */
public class AreYouSure {

  /**
   * Prompts the user with a confirmation message to confirm an action.
   *
   * @param force if {@code true}, skips user confirmation and proceeds.
   * @return {@code true} if the action is to continue {@code false} otherwise.
   */
  public static boolean really(boolean force) {
    /* force option for scripting */
    if (force) {
      return true;
    }

    try (Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name())) {
      System.out.println(
          "This command could result in data loss or other issues. Are you sure you want to do this? (Y/N)");
      String answer = scanner.next();
      return "Y".equals(answer);
    } catch (Exception e) {
      System.err.println("Error while reading user input: " + e.getMessage());
      Main.exit(-1);
    }
    return false;
  }
}
