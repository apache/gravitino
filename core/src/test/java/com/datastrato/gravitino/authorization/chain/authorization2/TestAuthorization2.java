/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain.authorization2;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.BaseAuthorization;
import java.util.Map;

public class TestAuthorization2 extends BaseAuthorization<TestAuthorization2> {

  public TestAuthorization2() {}

  @Override
  public String shortName() {
    return "test2";
  }

  @Override
  protected AuthorizationOperations newOps(Map<String, String> config) {
    return new TestAuthorizationOperations2();
  }
}
