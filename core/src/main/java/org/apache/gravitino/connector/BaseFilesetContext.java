/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.connector;

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetContext;

/**
 * An abstract class representing a base fileset context for {@link FilesetCatalog}. Developers
 * should extend this class to implement a custom fileset context for their fileset catalog.
 */
@Evolving
public abstract class BaseFilesetContext implements FilesetContext {
  protected Fileset fileset;
  protected String actualPath;

  @Override
  public Fileset fileset() {
    return fileset;
  }

  @Override
  public String actualPath() {
    return actualPath;
  }

  interface Builder<
      SELF extends BaseFilesetContext.Builder<SELF, T>, T extends BaseFilesetContext> {

    SELF withFileset(Fileset fileset);

    SELF withActualPath(String actualPath);

    T build();
  }

  /**
   * An abstract class implementing the builder interface for {@link BaseFilesetContext}. This class
   * should be extended by the concrete fileset context builders.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the fileset being built.
   */
  public abstract static class BaseFilesetContextBuilder<
          SELF extends BaseFilesetContext.Builder<SELF, T>, T extends BaseFilesetContext>
      implements BaseFilesetContext.Builder<SELF, T> {
    protected Fileset fileset;
    protected String actualPath;

    /**
     * Sets the fileset of the fileset context.
     *
     * @param fileset The fileset object.
     * @return The builder instance.
     */
    @Override
    public SELF withFileset(Fileset fileset) {
      this.fileset = fileset;
      return self();
    }

    /**
     * Sets the actual path of the fileset context.
     *
     * @param actualPath The actual path for the fileset context.
     * @return The builder instance.
     */
    @Override
    public SELF withActualPath(String actualPath) {
      this.actualPath = actualPath;
      return self();
    }

    /**
     * Builds the instance of the fileset with the provided attributes.
     *
     * @return The built fileset instance.
     */
    @Override
    public T build() {
      T t = internalBuild();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    /**
     * Builds the concrete instance of the fileset with the provided attributes.
     *
     * @return The built fileset instance.
     */
    @Evolving
    protected abstract T internalBuild();
  }
}
