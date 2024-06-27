/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain.authorization2;

import com.datastrato.gravitino.authorization.AuthorizationHook;
import com.datastrato.gravitino.authorization.BaseAuthorization;

public class TestAuthorization2 extends BaseAuthorization<TestAuthorization2> {

  public TestAuthorization2() {}

  @Override
  public String shortName() {
    return "test2";
  }

  @Override
  protected AuthorizationHook newHook() {
    return new TestAuthorizationHook2();
  }
}
