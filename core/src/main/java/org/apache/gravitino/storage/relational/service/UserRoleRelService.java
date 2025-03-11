package org.apache.gravitino.storage.relational.service;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

public class UserRoleRelService {

  private static final UserRoleRelService INSTANCE = new UserRoleRelService();

  public static UserRoleRelService getInstance() {
    return INSTANCE;
  }

  public List<UserRoleRelPO> listAllUserRoleRel() {
    return SessionUtils.getWithoutCommit(UserRoleRelMapper.class, UserRoleRelMapper::listAll);
  }
}
