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

/** Plain format to print a pretty string to standard out. */
public class PlainFormat {
  public static void output(Object object) {
    if (object instanceof Metalake) {
      new MetalakePlainFormat().output((Metalake) object);
    } else if (object instanceof Metalake[]) {
      new MetalakesPlainFormat().output((Metalake[]) object);
    } else if (object instanceof Catalog) {
      new CatalogPlainFormat().output((Catalog) object);
    } else if (object instanceof Catalog[]) {
      new CatalogsPlainFormat().output((Catalog[]) object);
    } else {
      throw new IllegalArgumentException("Unsupported object type");
    }
  }

  static final class MetalakePlainFormat implements OutputFormat<Metalake> {
    @Override
    public void output(Metalake metalake) {
      System.out.println(metalake.name() + "," + metalake.comment());
    }
  }

  static final class MetalakesPlainFormat implements OutputFormat<Metalake[]> {
    @Override
    public void output(Metalake[] metalakes) {
      List<String> metalakeNames =
          Arrays.stream(metalakes).map(Metalake::name).collect(Collectors.toList());
      String all = String.join(System.lineSeparator(), metalakeNames);
      System.out.println(all);
    }
  }

  static final class CatalogPlainFormat implements OutputFormat<Catalog> {
    @Override
    public void output(Catalog catalog) {
      System.out.println(
          catalog.name()
              + ","
              + catalog.type()
              + ","
              + catalog.provider()
              + ","
              + catalog.comment());
    }
  }

  static final class CatalogsPlainFormat implements OutputFormat<Catalog[]> {
    @Override
    public void output(Catalog[] catalogs) {
      List<String> catalogNames =
          Arrays.stream(catalogs).map(Catalog::name).collect(Collectors.toList());
      String all = String.join(System.lineSeparator(), catalogNames);
      System.out.println(all);
    }
  }
}
