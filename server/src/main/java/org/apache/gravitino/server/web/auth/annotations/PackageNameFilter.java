package org.apache.gravitino.server.web.auth.annotations;

import org.glassfish.hk2.api.Descriptor;
import org.glassfish.hk2.api.Filter;

public class PackageNameFilter implements Filter {
  private final String packagePrefix;

  public PackageNameFilter(String packagePrefix) {
    // 确保包名以 '.' 结尾，以便正确匹配子包
    this.packagePrefix = packagePrefix.endsWith(".") ? packagePrefix : packagePrefix + ".";
  }

  @Override
  public boolean matches(Descriptor descriptor) {
    // 获取服务的实现类名称
    String implementation = descriptor.getImplementation();
    // 检查该实现类是否属于指定的包或其子包
    return implementation.startsWith(packagePrefix);
  }
}
