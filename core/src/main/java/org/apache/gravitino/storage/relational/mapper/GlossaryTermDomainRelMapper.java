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

package org.apache.gravitino.storage.relational.mapper;

import java.util.Collection;
import java.util.List;
import org.apache.gravitino.storage.relational.po.GlossaryTermDomainRelPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Options.FlushCachePolicy;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

/**
 * Glossary-specific projections and attachment guarded by a live Term in the same metalake. Generic
 * membership reads, retirement and garbage collection use {@link DomainMemberMapper}.
 */
public interface GlossaryTermDomainRelMapper {
  /** Inserts a live membership row. */
  @InsertProvider(type = GlossaryTermDomainRelSQLProviderFactory.class, method = "insert")
  int insert(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("domainId") long domainId,
      @Param("audit") String audit);

  /** Lists live memberships of a nonempty batch of terms with the names of live domains. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermDomainRelSQLProviderFactory.class, method = "listDomainNames")
  List<GlossaryTermDomainRelPO> listDomainNames(
      @Param("metalakeId") long metalakeId, @Param("termIds") Collection<Long> termIds);

  /** Lists the live member terms of a nonempty set of domains. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermDomainRelSQLProviderFactory.class, method = "listTermIds")
  List<Long> listTermIds(
      @Param("metalakeId") long metalakeId, @Param("domainIds") Collection<Long> domainIds);
}
