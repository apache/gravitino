/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import java.util.UUID;

/**
 * Random id generator. This is used to generate random ids for entities. Please see {@link
 * com.datastrato.gravitino.meta.BaseMetalake#ID} for more details.
 */
public class RandomIdGenerator implements IdGenerator {

  public static final RandomIdGenerator INSTANCE = new RandomIdGenerator();

  public static final long MAX_ID = 0x7fffffffffffffffL;

  @Override
  public long nextId() {
    // Make sure this is a positive number.
    return UUID.randomUUID().getLeastSignificantBits() & MAX_ID;
  }
}
