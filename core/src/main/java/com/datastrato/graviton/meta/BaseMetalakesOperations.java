/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.meta;

import com.datastrato.graviton.*;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import java.util.Map;

public class BaseMetalakesOperations implements SupportMetalakes {

  @Override
  public BaseMetalake[] listMetalakes() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BaseMetalake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BaseMetalake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BaseMetalake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
