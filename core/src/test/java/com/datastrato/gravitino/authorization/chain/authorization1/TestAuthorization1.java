/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain.authorization1;

import com.datastrato.gravitino.authorization.AuthorizationHook;
import com.datastrato.gravitino.authorization.BaseAuthorization;

public class TestAuthorization1 extends BaseAuthorization<TestAuthorization1> {

  public TestAuthorization1() {}

  @Override
  public String shortName() {
    return "test1";
  }

  @Override
  protected AuthorizationHook newHook() {
    return new TestAuthorizationHook1();
  }
}
