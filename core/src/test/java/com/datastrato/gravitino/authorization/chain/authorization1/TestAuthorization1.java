/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain.authorization1;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.BaseAuthorization;
import java.util.Map;

public class TestAuthorization1 extends BaseAuthorization<TestAuthorization1> {

  public TestAuthorization1() {}

  @Override
  public String shortName() {
    return "test1";
  }

  @Override
  protected AuthorizationOperations newOps(Map<String, String> config) {
    return new TestAuthorizationOperations1();
  }
}
