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

import java.util.List;
import org.apache.gravitino.domain.DefinitionMember;
import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * Shared persistence of explicit Domain memberships. Writers fence the member and Domain in their
 * transaction.
 */
public interface DomainMemberMapper {
  /** Table for explicit membership of definitions in Domains. */
  String TABLE_NAME = "domain_member_rel";

  /** Types sharing the Domain membership table. */
  enum MemberType {
    GLOSSARY_TERM,
    KNOWLEDGE_BASE_ARTICLE
  }

  /** Stable member and Domain identities for reverse lookups and bulk reads. */
  record Membership(MemberType memberType, long memberId, long domainId) {}

  /** A direct member's type and current display name. */
  record NamedMember(DefinitionMember.Type type, String name) implements DefinitionMember {}

  /** Checks all member types after the caller has acquired the exclusive Domain lock. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @SelectProvider(type = DomainMemberSQLProviderFactory.class, method = "hasMembers")
  boolean hasMembers(@Param("domain") long domain);

  /** Loads the complete Domain set of one typed member. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @SelectProvider(type = DomainMemberSQLProviderFactory.class, method = "domains")
  List<Long> domains(
      @Param("lake") long lake, @Param("type") MemberType type, @Param("member") long member);

  /** Loads memberships of one feature for a metalake snapshot. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @SelectProvider(type = DomainMemberSQLProviderFactory.class, method = "list")
  @ConstructorArgs({
    @Arg(column = "member_type", javaType = MemberType.class),
    @Arg(column = "member_id", javaType = long.class),
    @Arg(column = "domain_id", javaType = long.class)
  })
  List<Membership> list(@Param("lake") long lake, @Param("type") MemberType type);

  /** Lists direct members in stable order across both features. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @SelectProvider(type = DomainMemberSQLProviderFactory.class, method = "members")
  @ConstructorArgs({
    @Arg(column = "member_type", javaType = MemberType.class),
    @Arg(column = "member_id", javaType = long.class),
    @Arg(column = "domain_id", javaType = long.class)
  })
  List<Membership> members(
      @Param("lake") long lake,
      @Param("domain") long domain,
      @Param("limit") int limit,
      @Param("offset") int offset);

  /** Lists current names of direct definitions after filtering retired relations and targets. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @SelectProvider(type = DomainMemberSQLProviderFactory.class, method = "liveMembers")
  @ConstructorArgs({
    @Arg(column = "member_type", javaType = DefinitionMember.Type.class),
    @Arg(column = "member_name", javaType = String.class)
  })
  List<NamedMember> liveMembers(
      @Param("lake") long lake,
      @Param("domain") long domain,
      @Param("limit") int limit,
      @Param("offset") int offset);

  /** Adds a verified live member and Domain pair. */
  @InsertProvider(type = DomainMemberSQLProviderFactory.class, method = "add")
  void add(
      @Param("lake") long lake,
      @Param("type") MemberType type,
      @Param("member") long member,
      @Param("domain") long domain,
      @Param("audit") String audit);

  /** Returns the latest retirement for the locked member. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @SelectProvider(type = DomainMemberSQLProviderFactory.class, method = "lastRetirement")
  long lastRetirement(
      @Param("lake") long lake, @Param("type") MemberType type, @Param("member") long member);

  /** Retires a pair using the generation selected under the member lock. */
  @UpdateProvider(type = DomainMemberSQLProviderFactory.class, method = "retire")
  void retire(
      @Param("lake") long lake,
      @Param("type") MemberType type,
      @Param("member") long member,
      @Param("domain") long domain,
      @Param("now") long now);

  /** Retires all live memberships of one locked definition. */
  @UpdateProvider(type = DomainMemberSQLProviderFactory.class, method = "retireMember")
  void retireMember(
      @Param("lake") long lake,
      @Param("type") MemberType type,
      @Param("member") long member,
      @Param("now") long now);

  /** Returns the latest retained generation for a metalake. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @SelectProvider(type = DomainMemberSQLProviderFactory.class, method = "lastMetalakeRetirement")
  long lastMetalakeRetirement(@Param("lake") long lake);

  /** Retires all live pairs in the locked metalake. */
  @UpdateProvider(type = DomainMemberSQLProviderFactory.class, method = "retireMetalake")
  void retireMetalake(@Param("lake") long lake, @Param("now") long now);

  /** Collects a bounded batch of tombstones for both member types. */
  @DeleteProvider(type = DomainMemberSQLProviderFactory.class, method = "purge")
  int purge(@Param("cutoff") long cutoff, @Param("limit") int limit);
}
