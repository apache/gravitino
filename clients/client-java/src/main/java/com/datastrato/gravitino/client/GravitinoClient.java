/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.SupportsCatalogs;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gravitino Client for an user to interacte with the Gravitino API, allowing the client to list,
 * load, create, and alter Catalog.
 *
 * <p>It uses an underlying {@link RESTClient} to send HTTP requests and receive responses from the
 * API.
 */
public class GravitinoClient extends GravitinoClientBase implements SupportsCatalogs {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoClient.class);

  private final String metalake_name;

  private GravitinoMetaLake metaLake = null;

  /**
   * Constructs a new GravitinoClient with the given URI, authenticator and AuthDataProvider.
   *
   * @param uri The base URI for the Gravitino API.
   * @param metalake_name The specified metalake name.
   * @param authDataProvider The provider of the data which is used for authentication.
   */
  private GravitinoClient(String uri, String metalake_name, AuthDataProvider authDataProvider) {
    super(uri, authDataProvider);
    this.metalake_name = metalake_name;
  }

  public GravitinoMetaLake getMetaLake() {
    if (this.metaLake == null) {
      synchronized (GravitinoClient.class) {
        if (this.metaLake == null) {
          this.metaLake = loadMetalake(NameIdentifier.of(metalake_name));
        }
      }
    }

    return metaLake;
  }

  /**
   * Creates a new builder for constructing a GravitinoClient.
   *
   * @param uri The base URI for the Gravitino API.
   * @return A new instance of the Builder class for constructing a GravitinoClient.
   */
  public static Builder builder(String uri) {
    return new Builder(uri);
  }

  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    return getMetaLake().listCatalogs(namespace);
  }

  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    return getMetaLake().loadCatalog(ident);
  }

  @Override
  public Catalog createCatalog(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    return getMetaLake().createCatalog(ident, type, provider, comment, properties);
  }

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    return getMetaLake().alterCatalog(ident, changes);
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    return getMetaLake().dropCatalog(ident);
  }

  /** Builder class for constructing a GravitinoClient. */
  public static class Builder {

    private String uri;
    private AuthDataProvider authDataProvider;
    private String metalakeName;

    /**
     * The private constructor for the Builder class.
     *
     * @param uri The base URI for the Gravitino API.
     */
    private Builder(String uri) {
      this.uri = uri;
    }

    public Builder withMetalake(String metalakeName) {
      this.metalakeName = metalakeName;
      return this;
    }

    /**
     * Sets the simple mode authentication for Gravitino
     *
     * @return This Builder instance for method chaining.
     */
    public Builder withSimpleAuth() {
      this.authDataProvider = new SimpleTokenProvider();
      return this;
    }

    /**
     * Sets OAuth2TokenProvider for the GravitinoClient.
     *
     * @param dataProvider The OAuth2TokenProvider used as the provider of authentication data for
     *     GravitinoClient.
     * @return This Builder instance for method chaining.
     */
    public Builder withOAuth(OAuth2TokenProvider dataProvider) {
      this.authDataProvider = dataProvider;
      return this;
    }

    /**
     * Sets KerberosTokenProvider for the GravitinoClient.
     *
     * @param dataProvider The KerberosTokenProvider used as the provider of authentication data for
     *     GravitinoClient.
     * @return This Builder instance for method chaining.
     */
    public Builder withKerberosAuth(KerberosTokenProvider dataProvider) {
      try {
        if (uri != null) {
          dataProvider.setHost(new URI(uri).getHost());
        }
      } catch (URISyntaxException ue) {
        throw new IllegalArgumentException("URI has the wrong format", ue);
      }
      this.authDataProvider = dataProvider;
      return this;
    }

    /**
     * Builds a new GravitinoClient instance.
     *
     * @return A new instance of GravitinoClient with the specified base URI.
     * @throws IllegalArgumentException If the base URI is null or empty.
     */
    public GravitinoClient build() {
      Preconditions.checkArgument(
          uri != null && !uri.isEmpty(), "The argument 'uri' must be a valid URI");
      Preconditions.checkArgument(
          metalakeName != null && !metalakeName.isEmpty(),
          "The argument 'metalakeName' must be a valid name");

      return new GravitinoClient(uri, metalakeName, authDataProvider);
    }
  }
}
