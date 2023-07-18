/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/

package com.datastrato.graviton.meta;

import com.datastrato.graviton.*;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import java.util.Map;

public class BaseCatalogsOperations implements SupportCatalogs {

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
}
