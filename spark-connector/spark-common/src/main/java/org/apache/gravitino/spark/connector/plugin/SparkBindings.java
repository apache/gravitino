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
package org.apache.gravitino.spark.connector.plugin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.spark.connector.catalog.SparkCatalogKind;

/**
 * The classes a connector build supplies to {@link GravitinoDriverPlugin}: which class implements
 * each kind of catalog, and which session extension performs authorization checks.
 *
 * <p>Every one of these differs per Spark version, so each version module builds its own instance
 * in its {@code GravitinoSparkPlugin} and the shared plugin never names a version-specific class.
 * That keeps the dependency running from version module to shared code, and lets a test supply its
 * own bindings to exercise the registration logic.
 */
public final class SparkBindings {

  /**
   * The catalog kinds every connector build must supply. Paimon is absent because Paimon publishes
   * no artifact for every supported Spark and Scala version, so some builds legitimately ship no
   * Paimon catalog.
   */
  private static final Set<SparkCatalogKind> REQUIRED_KINDS =
      EnumSet.complementOf(EnumSet.of(SparkCatalogKind.LAKEHOUSE_PAIMON));

  private final Map<SparkCatalogKind, String> catalogClassNames;
  private final String authorizationExtension;

  private SparkBindings(Builder builder) {
    this.catalogClassNames = ImmutableMap.copyOf(builder.catalogClassNames);
    this.authorizationExtension = builder.authorizationExtension;
  }

  /**
   * Returns a builder for the bindings of one connector build.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the class name implementing each kind of catalog this build ships.
   *
   * @return catalog kind to Spark catalog class name
   */
  public Map<SparkCatalogKind, String> catalogClassNames() {
    return catalogClassNames;
  }

  /**
   * Returns the class name of the authorization session extension this build ships.
   *
   * @return the Spark session extension class name
   */
  public String authorizationExtension() {
    return authorizationExtension;
  }

  /** Collects the bindings of one connector build, and checks that none is missing. */
  public static final class Builder {

    private final Map<SparkCatalogKind, String> catalogClassNames =
        new EnumMap<>(SparkCatalogKind.class);
    @Nullable private String authorizationExtension;

    private Builder() {}

    /**
     * Binds a catalog kind to the class implementing it.
     *
     * @param kind the kind of catalog
     * @param catalogClass the class implementing it
     * @return this builder
     */
    public Builder catalog(SparkCatalogKind kind, Class<?> catalogClass) {
      return catalog(kind, catalogClass.getName());
    }

    /**
     * Binds a catalog kind to the class implementing it, by name. Prefer {@link
     * #catalog(SparkCatalogKind, Class)}, which the compiler checks; this overload is for a catalog
     * whose class a build may compile out, such as Paimon on Scala 2.13.
     *
     * @param kind the kind of catalog
     * @param catalogClassName the name of the class implementing it
     * @return this builder
     */
    public Builder catalog(SparkCatalogKind kind, String catalogClassName) {
      catalogClassNames.put(kind, catalogClassName);
      return this;
    }

    /**
     * Binds the session extension that performs authorization checks.
     *
     * @param extensionClass the session extension class
     * @return this builder
     */
    public Builder authorizationExtension(Class<?> extensionClass) {
      return authorizationExtension(extensionClass.getName());
    }

    /**
     * Binds the session extension that performs authorization checks, by name. Prefer {@link
     * #authorizationExtension(Class)}, which the compiler checks.
     *
     * @param extensionClassName the name of the session extension class
     * @return this builder
     */
    public Builder authorizationExtension(String extensionClassName) {
      this.authorizationExtension = extensionClassName;
      return this;
    }

    /**
     * Builds the bindings, failing if this build left one out.
     *
     * @return the bindings
     */
    public SparkBindings build() {
      Preconditions.checkState(
          authorizationExtension != null, "No authorization session extension was bound");
      Set<SparkCatalogKind> missing = EnumSet.copyOf(REQUIRED_KINDS);
      missing.removeAll(catalogClassNames.keySet());
      Preconditions.checkState(missing.isEmpty(), "No catalog was bound for %s", missing);
      return new SparkBindings(this);
    }
  }
}
