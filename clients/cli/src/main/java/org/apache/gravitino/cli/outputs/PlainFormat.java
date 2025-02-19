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
package org.apache.gravitino.cli.outputs;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.cli.CommandContext;

/** Plain format to print a pretty string to standard out. */
public abstract class PlainFormat<T> extends BaseOutputFormat<T> {

  /**
   * Routes the object to its appropriate formatter and outputs the formatted result. Creates a new
   * formatter instance for the given object type and delegates the formatting.
   *
   * @param entity The object to format
   * @param context The command context
   * @throws IllegalArgumentException if the object type is not supported
   */
  public static void output(Object entity, CommandContext context) {
    if (entity instanceof Metalake) {
      new MetalakePlainFormat(context).output((Metalake) entity);
    } else if (entity instanceof Metalake[]) {
      new MetalakeListPlainFormat(context).output((Metalake[]) entity);
    } else if (entity instanceof Catalog) {
      new CatalogPlainFormat(context).output((Catalog) entity);
    } else if (entity instanceof Catalog[]) {
      new CatalogListPlainFormat(context).output((Catalog[]) entity);
    } else {
      throw new IllegalArgumentException("Unsupported object type");
    }
  }

  /**
   * Creates a new {@link PlainFormat} with the specified output properties.
   *
   * @param context The command context.
   */
  public PlainFormat(CommandContext context) {
    super(context);
  }

  /**
   * Formats a single {@link Metalake} instance as a comma-separated string. Output format: name,
   * comment
   */
  static final class MetalakePlainFormat extends PlainFormat<Metalake> {

    public MetalakePlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Metalake metalake) {
      return COMMA_JOINER.join(metalake.name(), metalake.comment());
    }
  }

  /**
   * Formats an array of Metalakes, outputting one name per line. Returns null if the array is empty
   * or null.
   */
  static final class MetalakeListPlainFormat extends PlainFormat<Metalake[]> {

    public MetalakeListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Metalake[] metalakes) {
      if (metalakes == null || metalakes.length == 0) {
        return null;
      } else {
        List<String> metalakeNames =
            Arrays.stream(metalakes).map(Metalake::name).collect(Collectors.toList());
        return NEWLINE_JOINER.join(metalakeNames);
      }
    }
  }

  /**
   * Formats a single {@link Catalog} instance as a comma-separated string. Output format: name,
   * type, provider, comment
   */
  static final class CatalogPlainFormat extends PlainFormat<Catalog> {
    public CatalogPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Catalog catalog) {
      return COMMA_JOINER.join(
          catalog.name(), catalog.type(), catalog.provider(), catalog.comment());
    }
  }

  /**
   * Formats an array of Catalogs, outputting one name per line. Returns null if the array is empty
   * or null.
   */
  static final class CatalogListPlainFormat extends PlainFormat<Catalog[]> {
    public CatalogListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Catalog[] catalogs) {
      if (catalogs == null || catalogs.length == 0) {
        output("No catalogs exists.", System.err);
        return null;
      } else {
        List<String> catalogNames =
            Arrays.stream(catalogs).map(Catalog::name).collect(Collectors.toList());
        return NEWLINE_JOINER.join(catalogNames);
      }
    }
  }
}
