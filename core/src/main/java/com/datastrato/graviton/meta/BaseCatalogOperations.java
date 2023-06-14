package com.datastrato.graviton.meta;

import com.datastrato.graviton.EntityOperations;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.NoSuchEntityException;

public abstract class BaseCatalogOperations
    implements EntityOperations<BaseCatalog, CatalogCreate, CatalogChange> {

  @Override
  public BaseCatalog[] listEntities() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BaseCatalog loadEntity(NameIdentifier ident) throws NoSuchEntityException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BaseCatalog alterEntity(NameIdentifier ident, CatalogChange change)
      throws NoSuchEntityException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropEntity(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
