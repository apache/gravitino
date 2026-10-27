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
package org.apache.gravitino.utils;

import java.util.function.Function;

/** Utilities for resolving names from ordered rename changes. */
public final class RenameUtils {

  private RenameUtils() {}

  /**
   * Returns the name from the last rename change, or the original name if there is no rename.
   *
   * <p>This resolves only the name, not namespace changes such as moving a table to another schema.
   *
   * @param originalName the name before applying changes
   * @param changes the changes in application order
   * @param renameType the type of rename change
   * @param nameExtractor extracts the new name from a rename change
   * @param <C> the change type
   * @param <R> the rename change type
   * @return the final name after applying the rename changes
   */
  public static <C, R extends C> String getLastRenameName(
      String originalName, C[] changes, Class<R> renameType, Function<R, String> nameExtractor) {
    String name = originalName;
    for (C change : changes) {
      if (renameType.isInstance(change)) {
        name = nameExtractor.apply(renameType.cast(change));
      }
    }
    return name;
  }
}
