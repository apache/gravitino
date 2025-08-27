package org.apache.gravitino.cli.commands;

import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

public class OwnerDetails extends Command {

  protected final String metalake;
  protected final String entity;
  protected final MetadataObject.Type entityType;

  public OwnerDetails(CommandContext context, String metalake, String entity, String entityType) {
    super(context);
    this.metalake = metalake;
    this.entity = entity;

    if (entityType.equals(CommandEntities.METALAKE)) {
      this.entityType = MetadataObject.Type.METALAKE;
    } else if (entityType.equals(CommandEntities.CATALOG)) {
      this.entityType = MetadataObject.Type.CATALOG;
    } else if (entityType.equals(CommandEntities.SCHEMA)) {
      this.entityType = MetadataObject.Type.SCHEMA;
    } else if (entityType.equals(CommandEntities.TABLE)) {
      this.entityType = MetadataObject.Type.TABLE;
    } else if (entityType.equals(CommandEntities.COLUMN)) {
      this.entityType = MetadataObject.Type.COLUMN;
    } else {
      this.entityType = null;
    }
  }

  @Override
  public void handle() {
    if (entityType == null) {
      exitWithError(ErrorMessages.UNKNOWN_ENTITY);
    }

    Optional<Owner> owner = Optional.empty();
    MetadataObject metadata = MetadataObjects.parse(entity, entityType);

    try {
      GravitinoClient client = buildClient(metalake);
      owner = client.getOwner(metadata);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchMetadataObjectException err) {
      exitWithError(ErrorMessages.UNKNOWN_ENTITY);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (owner.isPresent()) {
      printResults(owner.get().name());
    } else {
      printInformation("No owner");
    }
  }

  @Override
  public Command validate() {
    if (entityType == null) exitWithError(ErrorMessages.UNKNOWN_ENTITY);
    return this;
  }
}
