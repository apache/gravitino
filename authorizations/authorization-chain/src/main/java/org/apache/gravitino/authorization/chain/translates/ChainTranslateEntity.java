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
package org.apache.gravitino.authorization.chain.translates;

import java.util.Objects;
import org.apache.gravitino.CatalogProvider;
import org.apache.gravitino.connector.authorization.AuthorizationPluginProvider;

/**
 * The ChainTranslateEntity class presents a chain translation entity that contains the from and to
 * catalog and plugin provider.
 */
public class ChainTranslateEntity {
  CatalogProvider.CatalogName from;
  CatalogProvider.CatalogName to;

  public ChainTranslateEntity(
      CatalogProvider.CatalogName fromCatalogName,
      CatalogProvider.CatalogName toCatalogName) {
    this.from = fromCatalogName;
    this.to = toCatalogName;
  }

  public ChainTranslateEntity(
      String fromCatalogName,
      String toCatalogName) {
    CatalogProvider.CatalogName fromCatalog = CatalogProvider.CatalogName.valueOf(fromCatalogName);
    CatalogProvider.CatalogName toCatalog = CatalogProvider.CatalogName.valueOf(toCatalogName);
    new ChainTranslateEntity(fromCatalog, toCatalog);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ChainTranslateEntity)) return false;
    ChainTranslateEntity that = (ChainTranslateEntity) o;
    return Objects.equals(from, that.from) && Objects.equals(to, that.to);
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to);
  }
}
