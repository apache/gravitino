/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.metalake;

import static com.datastrato.gravitino.Entity.SYSTEM_METALAKE_RESERVED_NAME;

import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class MetalakeNormalizeDispatcher implements MetalakeDispatcher {
  private static final Set<String> RESERVED_WORDS = ImmutableSet.of(SYSTEM_METALAKE_RESERVED_NAME);
  /**
   * Regular expression explanation:
   *
   * <p>^[a-zA-Z_] - Starts with a letter, digit, or underscore
   *
   * <p>[a-zA-Z0-9_]{0,63} - Followed by 0 to 63 characters (making the total length at most 64) of
   * letters (both cases), digits, underscores
   *
   * <p>$ - End of the string
   */
  private static final String METALAKE_NAME_PATTERN = "^\\w[\\w]{0,63}$";

  private final MetalakeDispatcher dispatcher;

  public MetalakeNormalizeDispatcher(MetalakeDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public Metalake[] listMetalakes() {
    return dispatcher.listMetalakes();
  }

  @Override
  public Metalake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    return dispatcher.loadMetalake(ident);
  }

  @Override
  public boolean metalakeExists(NameIdentifier ident) {
    return dispatcher.metalakeExists(ident);
  }

  @Override
  public Metalake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    validateMetalakeName(ident.name());
    return dispatcher.createMetalake(ident, comment, properties);
  }

  @Override
  public Metalake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    Arrays.stream(changes)
        .forEach(
            change -> {
              if (change instanceof MetalakeChange.RenameMetalake) {
                validateMetalakeName(((MetalakeChange.RenameMetalake) change).getNewName());
              }
            });
    return dispatcher.alterMetalake(ident, changes);
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident) {
    // For compatibility reasons, we only validate the metalake name when creating and altering a
    // metalake.
    return dispatcher.dropMetalake(ident);
  }

  private void validateMetalakeName(String name) {
    if (RESERVED_WORDS.contains(name)) {
      throw new IllegalArgumentException("The metalake name '" + name + "' is reserved.");
    }
    if (!name.matches(METALAKE_NAME_PATTERN)) {
      throw new IllegalArgumentException("The metalake name '" + name + "' is illegal.");
    }
  }
}
