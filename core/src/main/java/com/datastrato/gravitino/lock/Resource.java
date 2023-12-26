/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;

public interface Resource<R extends Resource<R>> {

  NameIdentifier resourceIdentifier();

  R parent();

  void addChild(R r);

  R getChild(NameIdentifier name);

  void setParent(R r);

  void setIdentifier(NameIdentifier name);
}
