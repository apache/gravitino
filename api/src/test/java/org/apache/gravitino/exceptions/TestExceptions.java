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
package org.apache.gravitino.exceptions;

import static org.apache.gravitino.exceptions.NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExceptions {

  @Test
  public void testNoSuchEntityExceptionIsInGravitinoHierarchy() {
    // Before the fix, NoSuchEntityException extended RuntimeException directly, outside the
    // NotFoundException hierarchy every sibling NoSuch* exception uses, so a raw escape mapped to
    // HTTP 500 instead of 404. Locals are typed Object so the instanceof checks below are evaluated
    // at runtime against the actual hierarchy (error-prone's BadInstanceof rejects a tautological
    // check on a statically-known subtype).
    Object noCause = new NoSuchEntityException(NO_SUCH_ENTITY_MESSAGE, "table", "a.b.c");
    Object withCause =
        new NoSuchEntityException(
            new IllegalStateException("cause"), NO_SUCH_ENTITY_MESSAGE, "table", "a.b.c");
    // NotFoundException is the parent that drives the 404 mapping; pin it directly rather than only
    // the broader GravitinoRuntimeException.
    Assertions.assertTrue(noCause instanceof NotFoundException);
    Assertions.assertTrue(withCause instanceof NotFoundException);
    Assertions.assertTrue(noCause instanceof GravitinoRuntimeException);
    Assertions.assertTrue(withCause instanceof GravitinoRuntimeException);
  }

  @Test
  public void testDomainExceptionsHierarchy() {
    // The parents drive the HTTP mapping: AlreadyExistsException to 409, NotFoundException to 404.
    Object alreadyBound = new BackingTagAlreadyBoundException("Tag %s already backs %s", "t", "d");
    Object alreadyExists = new DomainAlreadyExistsException("Domain %s already exists", "d");
    Object noSuchDomain = new NoSuchDomainException("Domain %s does not exist", "d");
    Assertions.assertTrue(alreadyBound instanceof AlreadyExistsException);
    Assertions.assertTrue(alreadyExists instanceof AlreadyExistsException);
    Assertions.assertTrue(noSuchDomain instanceof NotFoundException);
    Assertions.assertEquals(
        "Tag t already backs d", ((BackingTagAlreadyBoundException) alreadyBound).getMessage());
  }

  @Test
  public void testGlossaryExceptionsHierarchy() {
    // The parents drive the HTTP mapping: AlreadyExistsException to 409, NotFoundException to 404.
    Object alreadyExists = new TermAlreadyExistsException("Term %s already exists", "Churn");
    Object noSuchTerm = new NoSuchTermException("Term %s does not exist", "Churn");
    Assertions.assertTrue(alreadyExists instanceof AlreadyExistsException);
    Assertions.assertTrue(noSuchTerm instanceof NotFoundException);
    Assertions.assertEquals(
        "Term Churn does not exist", ((NoSuchTermException) noSuchTerm).getMessage());
  }

  /** The exception parents preserve conflict/invalid-query categories and diagnostic causes. */
  @Test
  public void testGlossaryConflictAndLookupExceptions() {
    Throwable cause = new IllegalStateException("store failure");
    TermVersionConflictException conflict =
        new TermVersionConflictException("Term %s changed; reload before editing", "Churn");
    Object conflictCategory = conflict;
    Assertions.assertTrue(conflictCategory instanceof OptimisticLockException);
    Assertions.assertEquals("Term Churn changed; reload before editing", conflict.getMessage());
    TermVersionConflictException conflictWithCause =
        new TermVersionConflictException(cause, "Term %s changed", "Churn");
    Assertions.assertSame(cause, conflictWithCause.getCause());
    Assertions.assertEquals("Term Churn changed", conflictWithCause.getMessage());

    LookupTooBroadException lookup =
        new LookupTooBroadException("Lookup is too broad; %s", "omit bindings or narrow the query");
    Object lookupCategory = lookup;
    Assertions.assertTrue(lookupCategory instanceof IllegalArgumentException);
    Assertions.assertEquals(
        "Lookup is too broad; omit bindings or narrow the query", lookup.getMessage());
    LookupTooBroadException lookupWithCause =
        new LookupTooBroadException(cause, "Lookup is too broad; %s", "narrow the query");
    Assertions.assertSame(cause, lookupWithCause.getCause());
    Assertions.assertEquals("Lookup is too broad; narrow the query", lookupWithCause.getMessage());
  }
}
