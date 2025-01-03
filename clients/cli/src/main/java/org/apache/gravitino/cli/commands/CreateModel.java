package org.apache.gravitino.cli.commands;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.*;
import org.apache.gravitino.model.ModelCatalog;

/** Creates a new model. */
public class CreateModel extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String model;
  protected final String comment;

  /**
   *
   * Creates a new model.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param comment The comment for the model.
   */
  public CreateModel(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String schema,
      String model,
      String comment) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.model = model;
    this.comment = comment;
  }

  /** Creates a new model */
  public void handle() {
    try (GravitinoClient client = buildClient(metalake)) {
      NameIdentifier name = NameIdentifier.of(schema, model);
      client.loadCatalog(catalog).asModelCatalog().registerModel(name, comment, null);
    } catch (NoSuchMetalakeException noSuchMetalakeException) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException noSuchCatalogException) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException noSuchSchemaException) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (ModelAlreadyExistsException modelAlreadyExistsException) {
      exitWithError(ErrorMessages.MODEL_EXISTS);
    } catch (Exception err) {
      exitWithError(err.getMessage());
    }

    System.out.println(model + " created");
  }
}
