package org.apache.gravitino.storage.relational.mapper;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class UserRoleRelSQLProvider {

  private static final Map<JDBCBackendType, UserRoleRelBaseProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new UserRoleRelMySQLProvider(),
              JDBCBackendType.H2, new UserRoleRelH2Provider(),
              JDBCBackendType.PG, new UserRoleRelPGProvider());

  public static UserRoleRelBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class UserRoleRelMySQLProvider extends UserRoleRelBaseProvider {}

  static class UserRoleRelH2Provider extends UserRoleRelBaseProvider {}

  static class UserRoleRelPGProvider extends UserRoleRelBaseProvider {}

  public String batchInsertUserRoleRel(@Param("userRoleRels") List<UserRoleRelPO> userRoleRelPOs) {
    return getProvider().batchInsertUserRoleRel(userRoleRelPOs);
  }

  public String batchInsertUserRoleRelOnDuplicateKeyUpdate(
      @Param("userRoleRels") List<UserRoleRelPO> userRoleRelPOs) {
    return getProvider().batchInsertUserRoleRelOnDuplicateKeyUpdate(userRoleRelPOs);
  }

  public String softDeleteUserRoleRelByUserId(@Param("userId") Long userId) {
    return getProvider().softDeleteUserRoleRelByUserId(userId);
  }

  public String softDeleteUserRoleRelByUserAndRoles(
      @Param("userId") Long userId, @Param("roleIds") List<Long> roleIds) {
    return getProvider().softDeleteUserRoleRelByUserAndRoles(userId, roleIds);
  }

  public String softDeleteUserRoleRelByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteUserRoleRelByMetalakeId(metalakeId);
  }

  public String softDeleteUserRoleRelByRoleId(@Param("roleId") Long roleId) {
    return getProvider().softDeleteUserRoleRelByRoleId(roleId);
  }

  public String deleteUserRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteUserRoleRelMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
