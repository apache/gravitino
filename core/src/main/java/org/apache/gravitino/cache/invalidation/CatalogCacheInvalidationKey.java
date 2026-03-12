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

package org.apache.gravitino.cache.invalidation;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.gravitino.NameIdentifier;

/** Invalidation key for catalog cache entries. */
public final class CatalogCacheInvalidationKey {
  private final NameIdentifier nameIdentifier;

  private CatalogCacheInvalidationKey(NameIdentifier nameIdentifier) {
    this.nameIdentifier =
        Preconditions.checkNotNull(nameIdentifier, "nameIdentifier cannot be null");
  }

  public static CatalogCacheInvalidationKey of(NameIdentifier nameIdentifier) {
    return new CatalogCacheInvalidationKey(nameIdentifier);
  }

  public NameIdentifier nameIdentifier() {
    return nameIdentifier;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CatalogCacheInvalidationKey)) {
      return false;
    }
    CatalogCacheInvalidationKey that = (CatalogCacheInvalidationKey) o;
    return Objects.equals(nameIdentifier, that.nameIdentifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nameIdentifier);
  }
}
