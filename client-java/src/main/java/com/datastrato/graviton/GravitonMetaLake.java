package com.datastrato.graviton;

import com.datastrato.graviton.client.RESTClient;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.google.common.base.Preconditions;
import java.util.Map;

public class GravitonMetaLake extends MetalakeDTO implements SupportCatalogs {

  private final RESTClient restClient;

  GravitonMetaLake(
      String name,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, comment, properties, auditDTO);
    this.restClient = restClient;
  }

  @Override
  public Catalog[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Catalog createCatalog(
      NameIdentifier ident, Catalog.Type type, String comment, Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  static class Builder extends MetalakeDTO.Builder<Builder> {
    private RESTClient restClient;

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public GravitonMetaLake build() {
      Preconditions.checkNotNull(restClient, "restClient must be set");
      Preconditions.checkArgument(
          name != null && !name.isEmpty(), "name must not be null or empty");
      Preconditions.checkArgument(audit != null, "audit must be non-null");
      return new GravitonMetaLake(name, comment, properties, audit, restClient);
    }
  }
}
