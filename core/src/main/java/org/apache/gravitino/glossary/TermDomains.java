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

package org.apache.gravitino.glossary;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.service.GlossaryTermDomainService;

/** The net change to one term's domain membership, calculated while its locks are held. */
final class TermDomains {
  record Edit(String domain, boolean add) {
    @Nullable
    static Edit from(TermChange change) {
      if (change instanceof TermChange.AddDomain c) {
        return new Edit(c.getDomain(), true);
      }
      if (change instanceof TermChange.RemoveDomain c) {
        return new Edit(c.getDomain(), false);
      }
      return null;
    }
  }

  private static final GlossaryTermDomainService DOMAINS = GlossaryTermDomainService.getInstance();
  private final long lake;
  private final long termId;
  private final Map<Long, String> before;
  private final Map<Long, String> after;

  TermDomains(long lake, long termId, Map<Long, String> current) {
    this.lake = lake;
    this.termId = termId;
    this.before = Map.copyOf(current);
    this.after = new LinkedHashMap<>(current);
  }

  /** Applies one edit, in request order, to a domain resolved and locked by the caller. */
  void edit(Edit edit, long domainId, String domainName) {
    if (edit.add()) {
      after.put(domainId, domainName);
    } else {
      after.remove(domainId);
    }
  }

  boolean changed() {
    return !before.keySet().equals(after.keySet());
  }

  /** Returns the original membership using the names observed under the domain locks. */
  String[] originalNames() {
    return new TreeSet<>(before.values()).toArray(new String[0]);
  }

  /** Returns the final domain names in ascending order. */
  String[] names() {
    return new TreeSet<>(after.values()).toArray(new String[0]);
  }

  /** Writes the net change in the caller's transaction. */
  void persist() {
    for (Long id : after.keySet()) {
      if (!before.containsKey(id)) {
        DOMAINS.add(lake, termId, id);
      }
    }
    for (Long id : before.keySet()) {
      if (!after.containsKey(id)) {
        DOMAINS.remove(lake, termId, id);
      }
    }
  }
}
