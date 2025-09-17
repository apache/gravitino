/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.cli.handler;

import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.MainCli;
import org.apache.gravitino.cli.outputs.OutputFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class TestCatalogCommand {
  @Test
  void testHelpCommandOfCatalog() {
    String[] args1 = {CommandEntities.CATALOG, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(args1));

    String[] args2 = {CommandEntities.CATALOG};
    Assertions.assertDoesNotThrow(() -> MainCli.main(args2));
  }

  @Test
  void testPrintCatalogCommandHelp() {
    String[] createArgs = {CommandEntities.CATALOG, CommandActions.CREATE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(createArgs));

    String[] detailsArgs = {CommandEntities.CATALOG, CommandActions.DETAILS, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(detailsArgs));

    String[] listArgs = {CommandEntities.CATALOG, CommandActions.LIST, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(listArgs));

    String[] deleteArgs = {CommandEntities.CATALOG, CommandActions.DELETE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(deleteArgs));

    String[] updateArgs = {CommandEntities.CATALOG, CommandActions.UPDATE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(updateArgs));

    String[] setArgs = {CommandEntities.CATALOG, CommandActions.SET, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(setArgs));

    String[] propertiesArgs = {CommandEntities.CATALOG, CommandActions.PROPERTIES, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(propertiesArgs));

    String[] removeArgs = {CommandEntities.CATALOG, CommandActions.REMOVE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(removeArgs));
  }

  @Test
  void testCreateCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogCreate mockCatalogCreate =
        new org.apache.gravitino.cli.handler.MockCatalogCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockCatalogCreate);
    String[] args = {
      "mock_create",
      "--provider",
      "HIVE",
      "--name",
      "catalog1",
      "--metalake",
      "ml1",
      "--comment",
      "hive catalog",
      "--properties",
      "key1=value1,key2=value2"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogCreate.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogCreate.commonOptions.metalake);
    Assertions.assertEquals(CatalogCreate.Provider.HIVE, mockCatalogCreate.provider);
    Assertions.assertEquals("hive catalog", mockCatalogCreate.comment);
    Assertions.assertEquals("value1", mockCatalogCreate.properties.get("key1"));
    Assertions.assertEquals("value2", mockCatalogCreate.properties.get("key2"));
  }

  @Test
  void testCreateCatalogWithDefault() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogCreate mockCatalogCreate =
        new org.apache.gravitino.cli.handler.MockCatalogCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockCatalogCreate);
    String[] args = {
      "mock_create", "--provider", "HIVE", "--name", "catalog1", "--metalake", "ml1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogCreate.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogCreate.commonOptions.metalake);
    Assertions.assertEquals(CatalogCreate.Provider.HIVE, mockCatalogCreate.provider);
    Assertions.assertEquals("", mockCatalogCreate.comment);
    Assertions.assertEquals(0, mockCatalogCreate.properties.size());
  }

  @Test
  void testCreateCatalogWithInvalidProvider() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogCreate mockCatalogCreate =
        new org.apache.gravitino.cli.handler.MockCatalogCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockCatalogCreate);
    String[] args = {
      "mock_create", "--provider", "invalid", "--name", "catalog1", "--metalake", "ml1"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.ParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testCreateCatalogWithoutProvider() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogCreate mockCatalogCreate =
        new org.apache.gravitino.cli.handler.MockCatalogCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockCatalogCreate);
    String[] args = {"mock_create", "--name", "catalog1", "--metalake", "ml1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testCreateCatalogWithMalformedEntity() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogCreate mockCatalogCreate =
        new org.apache.gravitino.cli.handler.MockCatalogCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockCatalogCreate);
    String[] args = {"mock_create", "--provider", "hive", "--metalake", "ml1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.ParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testGetDetailsOfCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogDetails mockCatalogDetails =
        new org.apache.gravitino.cli.handler.MockCatalogDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockCatalogDetails);
    String[] args = {"mock_details", "--name", "catalog1", "--metalake", "ml1", "--no-audit"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogDetails.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogDetails.commonOptions.metalake);
    Assertions.assertFalse(mockCatalogDetails.audit);
  }

  @Test
  void testGetAuditOfCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogDetails mockCatalogDetails =
        new org.apache.gravitino.cli.handler.MockCatalogDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockCatalogDetails);
    String[] args = {"mock_details", "--name", "catalog1", "--metalake", "ml1", "--audit"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogDetails.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogDetails.commonOptions.metalake);
    Assertions.assertTrue(mockCatalogDetails.audit);
  }

  @Test
  void testGetDetailsOfCatalogWithoutName() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogDetails mockCatalogDetails =
        new org.apache.gravitino.cli.handler.MockCatalogDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockCatalogDetails);
    String[] args = {"mock_details", "--metalake", "ml1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testListCatalogs() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogList mockCatalogList =
        new org.apache.gravitino.cli.handler.MockCatalogList();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_list", mockCatalogList);
    String[] args = {"mock_list", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockCatalogList.commonOptions.metalake);
    Assertions.assertEquals(
        OutputFormat.OutputType.PLAIN, mockCatalogList.commonOptions.outputFormat);
  }

  @Test
  void testListPropertiesOfCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogProperties mockCatalogProperties =
        new org.apache.gravitino.cli.handler.MockCatalogProperties();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_properties", mockCatalogProperties);
    String[] args = {"mock_properties", "--name", "catalog1", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogProperties.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogProperties.commonOptions.metalake);
    Assertions.assertEquals(
        OutputFormat.OutputType.PLAIN, mockCatalogProperties.commonOptions.outputFormat);
  }

  @Test
  void testListPropertiesOfCatalogWithMalformedEntity() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogProperties mockCatalogProperties =
        new org.apache.gravitino.cli.handler.MockCatalogProperties();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_properties", mockCatalogProperties);
    String[] args = {"mock_delete", "properties", "--metalake", "ml1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testDeleteCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogDelete mockCatalogDelete =
        new org.apache.gravitino.cli.handler.MockCatalogDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockCatalogDelete);
    String[] args = {"mock_delete", "--name", "catalog1", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogDelete.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogDelete.commonOptions.metalake);
    Assertions.assertFalse(mockCatalogDelete.force);
  }

  @Test
  void testDeleteCatalogWithForce() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogDelete mockCatalogDelete =
        new org.apache.gravitino.cli.handler.MockCatalogDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockCatalogDelete);
    String[] args = {"mock_delete", "--name", "catalog1", "--metalake", "ml1", "--force"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogDelete.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogDelete.commonOptions.metalake);
    Assertions.assertTrue(mockCatalogDelete.force);
  }

  @Test
  void testDeleteCatalogWithoutName() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogDelete mockCatalogDelete =
        new org.apache.gravitino.cli.handler.MockCatalogDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockCatalogDelete);
    String[] args = {"catalog", "mock_delete", "--metalake", "ml1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testSetPropertyOfCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogSet mockCatalogSet =
        new org.apache.gravitino.cli.handler.MockCatalogSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockCatalogSet);
    String[] args = {
      "mock_set",
      "--name",
      "catalog1",
      "--metalake",
      "ml1",
      "--property",
      "key1",
      "--value",
      "value1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogSet.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogSet.commonOptions.metalake);
    Assertions.assertEquals("key1", mockCatalogSet.propertyOptions.property);
    Assertions.assertEquals("value1", mockCatalogSet.propertyOptions.value);
  }

  @Test
  void testSetPropertyOfCatalogWithoutName() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogSet mockCatalogSet =
        new org.apache.gravitino.cli.handler.MockCatalogSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockCatalogSet);
    String[] args = {"mock_set", "--metalake", "ml1", "--property", "key1", "--value", "value1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testSetPropertyOfCatalogWithoutProperty() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogSet mockCatalogSet =
        new org.apache.gravitino.cli.handler.MockCatalogSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockCatalogSet);
    String[] args = {"mock_set", "--name", "catalog1", "--metalake", "ml1", "--value", "value1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testSetPropertyOfCatalogWithoutValue() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogSet mockCatalogSet =
        new org.apache.gravitino.cli.handler.MockCatalogSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockCatalogSet);
    String[] args = {"mock_set", "--name", "catalog1", "--metalake", "ml1", "--property", "key1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testRemovePropertyOfCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogRemove mockCatalogRemove =
        new org.apache.gravitino.cli.handler.MockCatalogRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockCatalogRemove);
    String[] args = {
      "mock_remove", "--name", "catalog1", "--metalake", "ml1", "--property", "key1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogRemove.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogRemove.commonOptions.metalake);
    Assertions.assertEquals("key1", mockCatalogRemove.property);
  }

  @Test
  void testRemovePropertyOfCatalogWithoutName() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogRemove mockCatalogRemove =
        new org.apache.gravitino.cli.handler.MockCatalogRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockCatalogRemove);
    String[] args = {"mock_remove", "--metalake", "ml1", "--property", "key1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testRemovePropertyOfCatalogWithoutProperty() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogRemove mockCatalogRemove =
        new org.apache.gravitino.cli.handler.MockCatalogRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockCatalogRemove);
    String[] args = {"mock_remove", "--name", "catalog1", "--metalake", "ml1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testUpdateNameOfCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogUpdate mockCatalogUpdate =
        new org.apache.gravitino.cli.handler.MockCatalogUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockCatalogUpdate);
    String[] args = {
      "mock_update", "--name", "catalog1", "--metalake", "ml1", "--rename", "new_name"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogUpdate.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogUpdate.commonOptions.metalake);
    Assertions.assertEquals("new_name", mockCatalogUpdate.updateOptions.newName);
  }

  @Test
  void testUpdateCommentOfCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogUpdate mockCatalogUpdate =
        new org.apache.gravitino.cli.handler.MockCatalogUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockCatalogUpdate);
    String[] args = {
      "mock_update", "--name", "catalog1", "--metalake", "ml1", "--comment", "new_comment"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogUpdate.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogUpdate.commonOptions.metalake);
    Assertions.assertEquals("new_comment", mockCatalogUpdate.updateOptions.comment);
  }

  @Test
  void testEnableCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogUpdate mockCatalogUpdate =
        new org.apache.gravitino.cli.handler.MockCatalogUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockCatalogUpdate);
    String[] args = {"mock_update", "--name", "catalog1", "--metalake", "ml1", "--enable"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogUpdate.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogUpdate.commonOptions.metalake);
    Assertions.assertTrue(mockCatalogUpdate.updateOptions.enableDisableOptions.enable);
    Assertions.assertFalse(mockCatalogUpdate.updateOptions.enableDisableOptions.disable);
  }

  @Test
  void testDisableCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogUpdate mockCatalogUpdate =
        new org.apache.gravitino.cli.handler.MockCatalogUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockCatalogUpdate);
    String[] args = {"mock_update", "--name", "catalog1", "--metalake", "ml1", "--disable"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("catalog1", mockCatalogUpdate.commonOptions.name);
    Assertions.assertEquals("ml1", mockCatalogUpdate.commonOptions.metalake);
    Assertions.assertFalse(mockCatalogUpdate.updateOptions.enableDisableOptions.enable);
    Assertions.assertTrue(mockCatalogUpdate.updateOptions.enableDisableOptions.disable);
  }

  @Test
  void testEnableAndDisableCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogUpdate mockCatalogUpdate =
        new org.apache.gravitino.cli.handler.MockCatalogUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockCatalogUpdate);
    String[] args = {
      "mock_update", "--name", "catalog1", "--metalake", "ml1", "--enable", "--disable"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MaxValuesExceededException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testUpdateBothNameAndCommentOfCatalog() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogUpdate mockCatalogUpdate =
        new org.apache.gravitino.cli.handler.MockCatalogUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockCatalogUpdate);
    String[] args = {
      "mock_update",
      "--name",
      "catalog1",
      "--metalake",
      "ml1",
      "--rename",
      "new_name",
      "--comment",
      "new_comment"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MutuallyExclusiveArgsException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testUpdateCatalogWithoutName() {
    CatalogCliHandler root = new CatalogCliHandler();
    org.apache.gravitino.cli.handler.MockCatalogUpdate mockCatalogUpdate =
        new org.apache.gravitino.cli.handler.MockCatalogUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockCatalogUpdate);
    String[] args = {"mock_update", "--metalake", "ml1", "--rename", "new_name"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }
}

/** Mock class for CatalogCreate command. */
@picocli.CommandLine.Command(name = "mock_create", description = "Create a new catalog")
class MockCatalogCreate extends CatalogCreate {
  /** {@inheritDoc} */
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock class for CatalogDetails command. */
@picocli.CommandLine.Command(
    name = "mock_details",
    description = "Get details of a catalog or get audit information for a catalog")
class MockCatalogDetails extends CatalogDetails {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock class for CatalogList command. */
@picocli.CommandLine.Command(name = "mock_list", description = "List all catalogs in the metalake")
class MockCatalogList extends CatalogList {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** mock class for CatalogProperties command. */
@picocli.CommandLine.Command(name = "mock_properties", description = "List properties of a catalog")
class MockCatalogProperties extends CatalogProperties {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock class for CatalogDelete command. */
@picocli.CommandLine.Command(name = "mock_delete", description = "Delete a catalog")
class MockCatalogDelete extends CatalogDelete {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock class for CatalogSet command. */
@picocli.CommandLine.Command(name = "mock_set", description = "Set property of a catalog")
class MockCatalogSet extends CatalogSet {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock class for CatalogRemove command. */
@picocli.CommandLine.Command(name = "mock_remove", description = "Remove a property from a catalog")
class MockCatalogRemove extends CatalogRemove {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock class for Update command. */
@CommandLine.Command(name = "mock_update", description = "Update a catalog")
class MockCatalogUpdate extends CatalogUpdate {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}
