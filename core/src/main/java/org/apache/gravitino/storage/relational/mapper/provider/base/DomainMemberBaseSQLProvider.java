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

package org.apache.gravitino.storage.relational.mapper.provider.base;

/** SQL for explicit Domain memberships shared by Terms and Articles. */
public class DomainMemberBaseSQLProvider {
  /** Checks all member types after the caller has acquired the exclusive Domain lock. */
  public String hasMembers() {
    return "SELECT EXISTS (SELECT 1 FROM domain_member_rel WHERE domain_id=#{domain} AND deleted_at=0)";
  }

  /** Loads the complete Domain set of one typed member. */
  public String domains() {
    return """
      SELECT domain_id FROM domain_member_rel
      WHERE metalake_id=#{lake} AND member_type=#{type} AND member_id=#{member} AND deleted_at=0
      ORDER BY domain_id
      """;
  }

  /** Loads memberships of one feature for a metalake snapshot. */
  public String list() {
    return """
      SELECT member_type, member_id, domain_id FROM domain_member_rel
      WHERE metalake_id=#{lake} AND member_type=#{type} AND deleted_at=0
      """;
  }

  /** Lists direct members in stable order across both features. */
  public String members() {
    return """
      SELECT member_type, member_id, domain_id FROM domain_member_rel
      WHERE metalake_id=#{lake} AND domain_id=#{domain} AND deleted_at=0
      ORDER BY member_type, member_id LIMIT #{limit} OFFSET #{offset}
      """;
  }

  /** Adds a verified live member and Domain pair. */
  public String add() {
    return """
      INSERT INTO domain_member_rel (metalake_id, domain_id, member_type, member_id, audit_info)
      VALUES (#{lake}, #{domain}, #{type}, #{member}, #{audit})
      """;
  }

  /** Returns the latest retirement for the locked member. */
  public String lastRetirement() {
    return "SELECT COALESCE(MAX(deleted_at), 0) FROM domain_member_rel WHERE metalake_id=#{lake} AND member_type=#{type} AND member_id=#{member}";
  }

  /** Retires a pair using the generation selected under the member lock. */
  public String retire() {
    return """
      UPDATE domain_member_rel SET deleted_at=#{now}
      WHERE metalake_id=#{lake} AND domain_id=#{domain} AND member_type=#{type} AND member_id=#{member} AND deleted_at=0
      """;
  }

  /** Returns the latest retained generation for a metalake. */
  public String lastMetalakeRetirement() {
    return "SELECT COALESCE(MAX(deleted_at), 0) FROM domain_member_rel WHERE metalake_id=#{lake}";
  }

  /** Retires all live pairs in the locked metalake. */
  public String retireMetalake() {
    return "UPDATE domain_member_rel SET deleted_at=#{now} WHERE metalake_id=#{lake} AND deleted_at=0";
  }

  /** Collects a bounded batch of tombstones for both member types. */
  public String purge() {
    return """
      DELETE FROM domain_member_rel WHERE id IN (
        SELECT id FROM (
          SELECT id FROM domain_member_rel
          WHERE deleted_at>0 AND deleted_at<#{cutoff}
          ORDER BY deleted_at, id LIMIT #{limit}
        ) expired
      )
      """;
  }
}
