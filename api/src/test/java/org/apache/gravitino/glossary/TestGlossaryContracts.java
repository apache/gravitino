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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the public glossary defaults and value contracts. */
public class TestGlossaryContracts {

  /** Unsupported conditional edits must not reach the legacy write, even for a no-op. */
  @Test
  void testUnsupportedConditionalEditDoesNotWrite() {
    LegacyOperations operations = new LegacyOperations();
    for (TermChange change :
        new TermChange[] {
          TermChange.setStatus(GlossaryTerm.Status.APPROVED),
          TermChange.updateDefinition("old content")
        }) {
      Assertions.assertThrows(
          UnsupportedOperationException.class,
          () -> operations.alterTerm("Customer", "opaque-old-identity-revision-1", change));
    }
    Assertions.assertEquals(0, operations.writes);
  }

  @Test
  void testMissingTokenCannotFallBackToUnconditionalEdit() {
    LegacyOperations operations = new LegacyOperations();
    String missingToken = null;
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            operations.alterTerm("Customer", missingToken, TermChange.updateDefinition("content")));
    Assertions.assertEquals(0, operations.writes);
  }

  @Test
  void testBindingLookupDefaultsPreserveExistingRequest() {
    LegacyOperations operations = new LegacyOperations();
    String[] words = {"active user"};
    GlossaryTerm[] result = operations.matchTerms(words, "Sales", true);
    Assertions.assertSame(operations.matches, result);
    Assertions.assertEquals(1, operations.lookups);
    Assertions.assertArrayEquals(words, operations.words);
    Assertions.assertEquals("Sales", operations.domain);
  }

  /** Unsupported definition-only lookup must not execute an expensive binding query. */
  @Test
  void testUnsupportedDefinitionOnlyLookupDoesNotQuery() {
    LegacyOperations operations = new LegacyOperations();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> operations.matchTerms(new String[] {"active user"}, "Sales", false));
    Assertions.assertEquals(0, operations.lookups);
  }

  @Test
  void testBindingPresenceDistinguishesUnrequestedAndEmpty() {
    for (boolean included : new boolean[] {false, true}) {
      GlossaryTerm term =
          term(
              (proxy, method, args) -> {
                if (method.getName().equals("bindingsIncluded")) {
                  return included;
                }
                return InvocationHandler.invokeDefault(proxy, method, args);
              });
      Assertions.assertEquals(included, term.bindingsIncluded());
      Assertions.assertEquals(0, term.bindings().length);
      Assertions.assertFalse(term.bindingsTruncated());
    }
  }

  @Test
  void testTermPublicationValidation() {
    TermPublication p = new TermPublication(10L, 2L, TermPublication.State.SUBMITTED, 1L, 2L);
    Assertions.assertEquals(10L, p.termId());
    Assertions.assertEquals(2L, p.versionId());
    Assertions.assertEquals(TermPublication.State.SUBMITTED, p.state());
    Assertions.assertEquals(1L, p.publishedVersionId());
    Assertions.assertEquals(2L, p.candidateVersionId());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new TermPublication(10L, 0L, TermPublication.State.DRAFT, null, null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new TermPublication(10L, 1L, null, null, null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new TermPublication(10L, 1L, TermPublication.State.PUBLISHED, 0L, null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new TermPublication(10L, 1L, TermPublication.State.SUBMITTED, null, 0L));
  }

  private static GlossaryTerm term(InvocationHandler handler) {
    return (GlossaryTerm)
        Proxy.newProxyInstance(
            GlossaryTerm.class.getClassLoader(), new Class<?>[] {GlossaryTerm.class}, handler);
  }

  private static final class LegacyOperations implements GlossaryOperations {
    private final GlossaryTerm result =
        term(
            (proxy, method, args) -> {
              throw new UnsupportedOperationException();
            });
    private final GlossaryTerm[] matches = {result};
    private int writes;
    private int lookups;
    private String[] words;
    @Nullable private String domain;

    @Override
    public String[] listTerms() {
      throw new UnsupportedOperationException();
    }

    @Override
    public GlossaryTerm[] listTermsInfo() {
      throw new UnsupportedOperationException();
    }

    @Override
    public GlossaryTerm getTerm(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public GlossaryTerm createTerm(TermSpec spec) {
      throw new UnsupportedOperationException();
    }

    @Override
    public GlossaryTerm alterTerm(String name, TermChange... changes) {
      writes++;
      return result;
    }

    @Override
    public boolean dropTerm(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public GlossaryTerm[] matchTerms(String[] words, @Nullable String domain) {
      lookups++;
      this.words = words;
      this.domain = domain;
      return matches;
    }
  }
}
