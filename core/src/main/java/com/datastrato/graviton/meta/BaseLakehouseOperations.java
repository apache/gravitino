package com.datastrato.graviton.meta;

import com.datastrato.graviton.*;

public class BaseLakehouseOperations
    implements EntityOperations<Lakehouse, LakehouseCreate, LakehouseChange> {
  @Override
  public Lakehouse[] listEntities() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Lakehouse loadEntity(NameIdentifier ident) throws NoSuchEntityException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Lakehouse createEntity(NameIdentifier ident, LakehouseCreate create)
      throws EntityAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Lakehouse alterEntity(NameIdentifier ident, LakehouseChange change)
      throws NoSuchEntityException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropEntity(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
