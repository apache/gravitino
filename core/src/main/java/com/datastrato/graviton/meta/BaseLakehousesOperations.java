package com.datastrato.graviton.meta;

import com.datastrato.graviton.*;
import com.datastrato.graviton.exceptions.LakehouseAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchLakehouseException;
import java.util.Map;

public class BaseLakehousesOperations implements SupportLakehouses {

  @Override
  public BaseLakehouse[] listLakehouses() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BaseLakehouse loadLakehouse(NameIdentifier ident) throws NoSuchLakehouseException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BaseLakehouse createLakehouse(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws LakehouseAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public BaseLakehouse alterLakehouse(NameIdentifier ident, LakehouseChange... changes)
      throws NoSuchLakehouseException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean dropLakehouse(NameIdentifier ident) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
