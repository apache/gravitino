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

import com.google.common.base.Preconditions;
import java.util.function.Supplier;

/**
 * Limits one table load to metadata already stored by Gravitino.
 *
 * <p>This class lives on the server classpath and outside the catalog package, so the catalog
 * classloader and the auxiliary Lance REST classloader share one {@link ThreadLocal}. A standalone
 * Lance REST process cannot see that flag. It sends {@link #METADATA_ONLY_PARAM} and the Gravitino
 * server sets the scope around the load.
 */
public final class TableLoadScope {

  /** Query parameter that requests stored table metadata without opening the dataset. */
  public static final String METADATA_ONLY_PARAM = "metadataOnly";

  private static final ThreadLocal<Boolean> ACTIVE = new ThreadLocal<>();

  private TableLoadScope() {}

  /**
   * Runs {@code action} so a table load can return stored metadata. The scope is cleared when
   * {@code action} returns or throws, including when an outer scope is already active.
   *
   * @param action the metadata-only operation
   * @param <T> the result type
   * @return the result of {@code action}
   */
  public static <T> T call(Supplier<T> action) {
    Preconditions.checkArgument(action != null, "action must not be null");
    boolean previous = isActive();
    ACTIVE.set(Boolean.TRUE);
    try {
      return action.get();
    } finally {
      if (previous) {
        ACTIVE.set(Boolean.TRUE);
      } else {
        ACTIVE.remove();
      }
    }
  }

  /**
   * Returns whether the current thread asked for stored metadata only.
   *
   * @return true when {@link #call(Supplier)} is active on this thread
   */
  public static boolean isActive() {
    return Boolean.TRUE.equals(ACTIVE.get());
  }
}
