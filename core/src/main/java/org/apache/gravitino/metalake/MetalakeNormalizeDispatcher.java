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
package org.apache.gravitino.metalake;

import static org.apache.gravitino.Entity.SYSTEM_METALAKE_RESERVED_NAME;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForAlter;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.meta.BaseMetalake.PROPERTIES_METADATA;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;

public class MetalakeNormalizeDispatcher implements MetalakeDispatcher {
  private static final Set<String> RESERVED_WORDS = ImmutableSet.of(SYSTEM_METALAKE_RESERVED_NAME);
  /**
   * Regular expression explanation:
   *
   * <p>^[\w] - Starts with a letter, digit, or underscore
   *
   * <p>[\w]{0,63} - Followed by 0 to 63 characters (making the total length at most 64) of letters
   * (both cases), digits, underscores
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
    validatePropertyForCreate(PROPERTIES_METADATA, properties);
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
    Pair<Map<String, String>, Map<String, String>> alterProperty =
        getMetalakeAlterProperty(changes);
    validatePropertyForAlter(
        PROPERTIES_METADATA, alterProperty.getLeft(), alterProperty.getRight());
    return dispatcher.alterMetalake(ident, changes);
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident) {
    // For compatibility reasons, we only validate the metalake name when creating and altering a
    // metalake.
    return dispatcher.dropMetalake(ident);
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, MetalakeInUseException {
    return dispatcher.dropMetalake(ident, force);
  }

  @Override
  public void enableMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    dispatcher.enableMetalake(ident);
  }

  @Override
  public void disableMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    dispatcher.disableMetalake(ident);
  }

  private void validateMetalakeName(String name) {
    if (RESERVED_WORDS.contains(name)) {
      throw new IllegalArgumentException("The metalake name '" + name + "' is reserved.");
    }
    if (!name.matches(METALAKE_NAME_PATTERN)) {
      throw new IllegalArgumentException("The metalake name '" + name + "' is illegal.");
    }
  }

  private Pair<Map<String, String>, Map<String, String>> getMetalakeAlterProperty(
      MetalakeChange... metalakeChanges) {
    Map<String, String> upserts = Maps.newHashMap();
    Map<String, String> deletes = Maps.newHashMap();

    Arrays.stream(metalakeChanges)
        .forEach(
            metalakeChange -> {
              if (metalakeChange instanceof MetalakeChange.SetProperty) {
                MetalakeChange.SetProperty setProperty =
                    (MetalakeChange.SetProperty) metalakeChange;
                upserts.put(setProperty.getProperty(), setProperty.getValue());
              } else if (metalakeChange instanceof MetalakeChange.RemoveProperty) {
                MetalakeChange.RemoveProperty removeProperty =
                    (MetalakeChange.RemoveProperty) metalakeChange;
                deletes.put(removeProperty.getProperty(), removeProperty.getProperty());
              }
            });

    return Pair.of(upserts, deletes);
  }
}
