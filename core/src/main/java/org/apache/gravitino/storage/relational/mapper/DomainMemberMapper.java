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
import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

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

  /** Checks all member types after the caller has acquired the exclusive Domain lock. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @Select(
      "SELECT EXISTS (SELECT 1 FROM domain_member_rel WHERE domain_id=#{domain} AND deleted_at=0)")
  boolean hasMembers(@Param("domain") long domain);

  /** Loads the complete Domain set of one typed member. */
  @Select(
      """
      SELECT domain_id FROM domain_member_rel
      WHERE metalake_id=#{lake} AND member_type=#{type} AND member_id=#{member} AND deleted_at=0
      ORDER BY domain_id
      """)
  List<Long> domains(
      @Param("lake") long lake, @Param("type") MemberType type, @Param("member") long member);

  /** Loads memberships of one feature for a metalake snapshot. */
  @Select(
      """
      SELECT member_type, member_id, domain_id FROM domain_member_rel
      WHERE metalake_id=#{lake} AND member_type=#{type} AND deleted_at=0
      """)
  @ConstructorArgs({
    @Arg(column = "member_type", javaType = MemberType.class),
    @Arg(column = "member_id", javaType = long.class),
    @Arg(column = "domain_id", javaType = long.class)
  })
  List<Membership> list(@Param("lake") long lake, @Param("type") MemberType type);

  /** Lists direct members in stable order across both features. */
  @Select(
      """
      SELECT member_type, member_id, domain_id FROM domain_member_rel
      WHERE metalake_id=#{lake} AND domain_id=#{domain} AND deleted_at=0
      ORDER BY member_type, member_id LIMIT #{limit} OFFSET #{offset}
      """)
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

  /** Adds a verified live member and Domain pair. */
  @Insert(
      """
      INSERT INTO domain_member_rel (metalake_id, domain_id, member_type, member_id, audit_info)
      VALUES (#{lake}, #{domain}, #{type}, #{member}, #{audit})
      """)
  void add(
      @Param("lake") long lake,
      @Param("type") MemberType type,
      @Param("member") long member,
      @Param("domain") long domain,
      @Param("audit") String audit);

  /** Retires one live typed membership without colliding with a retained generation. */
  default void remove(long lake, MemberType type, long member, long domain, long now) {
    retire(
        lake,
        type,
        member,
        domain,
        Math.max(now, Math.addExact(lastRetirement(lake, type, member), 1)));
  }

  /** Returns the latest retirement for the locked member. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @Select(
      "SELECT COALESCE(MAX(deleted_at), 0) FROM domain_member_rel WHERE metalake_id=#{lake} AND member_type=#{type} AND member_id=#{member}")
  long lastRetirement(
      @Param("lake") long lake, @Param("type") MemberType type, @Param("member") long member);

  /** Retires a pair using the generation selected under the member lock. */
  @Update(
      """
      UPDATE domain_member_rel SET deleted_at=#{now}
      WHERE metalake_id=#{lake} AND domain_id=#{domain} AND member_type=#{type} AND member_id=#{member} AND deleted_at=0
      """)
  void retire(
      @Param("lake") long lake,
      @Param("type") MemberType type,
      @Param("member") long member,
      @Param("domain") long domain,
      @Param("now") long now);

  /** Retires the memberships of all definitions under the exclusive metalake fence. */
  default void dropMetalake(long lake, long now) {
    retireMetalake(lake, Math.max(now, Math.addExact(lastMetalakeRetirement(lake), 1)));
  }

  /** Returns the latest retained generation for a metalake. */
  @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
  @Select("SELECT COALESCE(MAX(deleted_at), 0) FROM domain_member_rel WHERE metalake_id=#{lake}")
  long lastMetalakeRetirement(@Param("lake") long lake);

  /** Retires all live pairs in the locked metalake. */
  @Update(
      "UPDATE domain_member_rel SET deleted_at=#{now} WHERE metalake_id=#{lake} AND deleted_at=0")
  void retireMetalake(@Param("lake") long lake, @Param("now") long now);

  /** Collects a bounded batch of tombstones for both member types. */
  @Delete(
      """
      DELETE FROM domain_member_rel WHERE id IN (
        SELECT id FROM (
          SELECT id FROM domain_member_rel
          WHERE deleted_at>0 AND deleted_at<#{cutoff}
          ORDER BY id LIMIT #{limit}
        ) expired
      )
      """)
  int purge(@Param("cutoff") long cutoff, @Param("limit") int limit);
}
