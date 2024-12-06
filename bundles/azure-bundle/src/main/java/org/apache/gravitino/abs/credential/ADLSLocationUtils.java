package org.apache.gravitino.abs.credential;

import java.net.URI;

public class ADLSLocationUtils {
  public static class ADLSLocationParts {
    private final String container;
    private final String accountName;
    private final String path;

    public ADLSLocationParts(String container, String accountName, String path) {
      this.container = container;
      this.accountName = accountName;
      this.path = path;
    }

    public String getContainer() {
      return container;
    }

    public String getAccountName() {
      return accountName;
    }

    public String getPath() {
      return path;
    }
  }

  public static ADLSLocationParts parseLocation(String location) {
    URI locationUri = URI.create(location);

    String[] authorityParts = locationUri.getAuthority().split("@");

    if (authorityParts.length <= 1) {
      throw new IllegalArgumentException("Invalid location: " + location);
    }

    return new ADLSLocationParts(
        authorityParts[0], authorityParts[1].split("\\.")[0], locationUri.getPath());
  }
}
