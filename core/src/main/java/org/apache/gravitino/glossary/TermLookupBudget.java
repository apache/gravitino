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

import org.apache.gravitino.exceptions.LookupTooBroadException;

/** Authorization work remaining for one exact-match request, including definition-only reads. */
final class TermLookupBudget {
  private static final int MAX_CANDIDATES = 10000;
  private int remaining = MAX_CANDIDATES;

  int remaining() {
    return remaining;
  }

  void consume(int candidates) {
    if (candidates > remaining) {
      throw exceeded();
    }
    remaining -= candidates;
  }

  static LookupTooBroadException exceeded() {
    return new LookupTooBroadException(
        "Glossary lookup is too broad. Omit embedded bindings or narrow the query.");
  }
}
