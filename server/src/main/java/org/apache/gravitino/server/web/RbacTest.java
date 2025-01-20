package org.apache.gravitino.server.web;

import org.casbin.jcasbin.main.Enforcer;

public class RbacTest {

  public static void main(String[] args) {
    Enforcer e =
        new Enforcer(
            "/Users/zhongyangyang/IdeaProjects/gravitino/server/src/main/resources/rbac_modal.conf",
            "/Users/zhongyangyang/IdeaProjects/gravitino/server/src/main/resources/rbac_policy.csv");
    boolean result = e.enforce("aaa", "ddd", "ddd");
    
    System.out.println(result);
  }
}
