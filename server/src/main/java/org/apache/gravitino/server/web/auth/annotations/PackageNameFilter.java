package org.apache.gravitino.server.web.auth.annotations;

import org.glassfish.hk2.api.Descriptor;
import org.glassfish.hk2.api.Filter;

public class PackageNameFilter implements Filter {
  private final String packagePrefix;

  public PackageNameFilter(String packagePrefix) {
    // Make sure the package name ends with '.' in order to correctly match sub-packages
    this.packagePrefix = packagePrefix.endsWith(".") ? packagePrefix : packagePrefix + ".";
  }

  @Override
  public boolean matches(Descriptor descriptor) {
    // Get the name of the service's implementation class
    String implementation = descriptor.getImplementation();
    // Checks if the implementation class belongs to the 
    // specified package or its subpackages
    return implementation.startsWith(packagePrefix);
  }
}
