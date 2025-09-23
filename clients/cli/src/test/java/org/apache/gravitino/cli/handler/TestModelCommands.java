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

import java.util.HashMap;
import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.MainCli;
import org.apache.gravitino.cli.outputs.OutputFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class TestModelCommands {
  @Test
  void testModelHelp() {
    String[] args1 = {CommandEntities.MODEL, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(args1));

    String[] args2 = {CommandEntities.MODEL};
    Assertions.assertDoesNotThrow(() -> MainCli.main(args2));
  }

  @Test
  void testPrintModelCommandHelp() {
    String[] createArgs = {CommandEntities.MODEL, CommandActions.CREATE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(createArgs));

    String[] detailsArgs = {CommandEntities.MODEL, CommandActions.DETAILS, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(detailsArgs));

    String[] listArgs = {CommandEntities.MODEL, CommandActions.LIST, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(listArgs));

    String[] deleteArgs = {CommandEntities.MODEL, CommandActions.DELETE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(deleteArgs));

    String[] setArgs = {CommandEntities.MODEL, CommandActions.SET, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(setArgs));

    String[] removeArgs = {CommandEntities.MODEL, CommandActions.REMOVE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(removeArgs));

    String[] updateArgs = {CommandEntities.MODEL, CommandActions.UPDATE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(updateArgs));
  }

  @Test
  void testRegisterModel() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelCreate mockModelCreate =
        new org.apache.gravitino.cli.handler.MockModelCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockModelCreate);
    String[] args = {
      "mock_create",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--comment",
      "testModel",
      "--properties",
      "k1=v1,k2=v2"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("testModel", mockModelCreate.comment);
    Assertions.assertEquals("v1", mockModelCreate.properties.get("k1"));
    Assertions.assertEquals("v2", mockModelCreate.properties.get("k2"));
    Assertions.assertEquals("ml1", mockModelCreate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelCreate.name);
  }

  @Test
  void testRegisterModelWithDefaults() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelCreate mockModelCreate =
        new org.apache.gravitino.cli.handler.MockModelCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockModelCreate);
    String[] args = {"mock_create", "--metalake", "ml1", "--name", "catalog1.schema1.model1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelCreate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelCreate.name);
    Assertions.assertEquals("", mockModelCreate.comment);
    Assertions.assertEquals(new HashMap<>(), mockModelCreate.properties);
  }

  @Test
  void testRegisterModelWithMalformedEntity() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelCreate mockModelCreate =
        new org.apache.gravitino.cli.handler.MockModelCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockModelCreate);
    String[] args = {"mock_create", "--metalake", "ml1", "--name", "catalog1.schema1"};

    int exitCode = cmd.execute(args);
    Assertions.assertNotEquals(0, exitCode);
  }

  @Test
  void testGetModelDetails() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelDetails mockModelDetails =
        new org.apache.gravitino.cli.handler.MockModelDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockModelDetails);
    String[] args = {
      "mock_details", "--metalake", "ml1", "--name", "catalog1.schema1.model1", "--output", "TABLE"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelDetails.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelDetails.name);
    Assertions.assertEquals(
        OutputFormat.OutputType.TABLE, mockModelDetails.commonOptions.outputFormat);
    Assertions.assertFalse(mockModelDetails.audit);
  }

  @Test
  void testGetModelAuditInfo() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelDetails mockModelDetails =
        new org.apache.gravitino.cli.handler.MockModelDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockModelDetails);
    String[] args = {
      "mock_details",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--audit",
      "--output",
      "TABLE"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelDetails.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelDetails.name);
    Assertions.assertEquals(
        OutputFormat.OutputType.TABLE, mockModelDetails.commonOptions.outputFormat);
    Assertions.assertTrue(mockModelDetails.audit);
  }

  @Test
  void testGetModelWithNegativeOption() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelDetails mockModelDetails =
        new org.apache.gravitino.cli.handler.MockModelDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockModelDetails);
    String[] args = {
      "mock_details",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--output",
      "TABLE",
      "--no-audit"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelDetails.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelDetails.name);
    Assertions.assertEquals(
        OutputFormat.OutputType.TABLE, mockModelDetails.commonOptions.outputFormat);
    Assertions.assertFalse(mockModelDetails.audit);
  }

  @Test
  void testGetModelWithMalformedName() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelDetails mockModelDetails =
        new org.apache.gravitino.cli.handler.MockModelDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockModelDetails);
    String[] args = {
      "mock_details",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1",
      "--audit",
      "--output",
      "TABLE"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertNotEquals(0, exitCode);
  }

  @Test
  void testListModels() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModeList mockModeList =
        new org.apache.gravitino.cli.handler.MockModeList();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_list", mockModeList);
    String[] args1 = {
      "mock_list", "--metalake", "ml1", "--name", "catalog1.schema1", "--output", "TABLE"
    };

    int exitCode1 = cmd.execute(args1);
    Assertions.assertEquals(0, exitCode1);
    Assertions.assertEquals("ml1", mockModeList.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockModeList.name);
    Assertions.assertEquals(OutputFormat.OutputType.TABLE, mockModeList.commonOptions.outputFormat);

    String[] args2 = {
      "mock_list", "--metalake", "ml1", "--name", "catalog1.schema1", "--output", "PLAIN"
    };

    int exitCode2 = cmd.execute(args2);
    Assertions.assertEquals(0, exitCode2);
    Assertions.assertEquals("ml1", mockModeList.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockModeList.name);
    Assertions.assertEquals(OutputFormat.OutputType.PLAIN, mockModeList.commonOptions.outputFormat);
  }

  @Test
  void testListModelWithMalformedEntity() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModeList mockModeList =
        new org.apache.gravitino.cli.handler.MockModeList();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_list", mockModeList);

    String[] args = {"mock_list", "--metalake", "ml1", "--name", "catalog1", "--output", "TABLE"};

    int exitCode = cmd.execute(args);
    Assertions.assertNotEquals(0, exitCode);
  }

  @Test
  void testDeleteModel() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelDelete mockModelDelete =
        new org.apache.gravitino.cli.handler.MockModelDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockModelDelete);
    String[] args1 = {"mock_delete", "--metalake", "ml1", "--name", "catalog1.schema1.model1"};

    int exitCode1 = cmd.execute(args1);
    Assertions.assertEquals(0, exitCode1);
    Assertions.assertEquals("ml1", mockModelDelete.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelDelete.name);
    Assertions.assertFalse(mockModelDelete.force);

    String[] args2 = {
      "mock_delete", "--metalake", "ml1", "--name", "catalog1.schema1.model1", "--force"
    };
    int exitCode2 = cmd.execute(args2);
    Assertions.assertEquals(0, exitCode2);
    Assertions.assertEquals("ml1", mockModelDelete.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelDelete.name);
    Assertions.assertTrue(mockModelDelete.force);
  }

  @Test
  void testDeleteModelWithMalformedEntity() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelDelete mockModelDelete =
        new org.apache.gravitino.cli.handler.MockModelDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockModelDelete);
    String[] args = {"mock_delete", "--metalake", "ml1", "--name", "catalog1.schema1"};

    int exitCode = cmd.execute(args);
    Assertions.assertNotEquals(0, exitCode);
  }

  @Test
  void testSetPropertyOfModelVersionByAlias() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelSet mockModelSet =
        new org.apache.gravitino.cli.handler.MockModelSet();

    picocli.CommandLine cmd = new picocli.CommandLine(root).addSubcommand("mock_set", mockModelSet);
    String[] args1 = {
      "mock_set",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--alias",
      "alias1",
      "--property",
      "k1",
      "--value",
      "v2"
    };

    int exitCode1 = cmd.execute(args1);
    Assertions.assertEquals(0, exitCode1);
    Assertions.assertEquals("ml1", mockModelSet.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelSet.name);
    Assertions.assertEquals("alias1", mockModelSet.versionOrAlias.alias);
    Assertions.assertEquals("k1", mockModelSet.propertyOptions.property);
    Assertions.assertEquals("v2", mockModelSet.propertyOptions.value);
  }

  @Test
  void testSetPropertyOfModelVersionByVersion() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelSet mockModelSet =
        new org.apache.gravitino.cli.handler.MockModelSet();

    picocli.CommandLine cmd = new picocli.CommandLine(root).addSubcommand("mock_set", mockModelSet);
    String[] args = {
      "mock_set",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--version",
      "2",
      "--property",
      "k1",
      "--value",
      "v2"
    };

    int exitCode1 = cmd.execute(args);
    Assertions.assertEquals(0, exitCode1);
    Assertions.assertEquals("ml1", mockModelSet.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelSet.name);
    Assertions.assertEquals(2, mockModelSet.versionOrAlias.version);
    Assertions.assertEquals("k1", mockModelSet.propertyOptions.property);
    Assertions.assertEquals("v2", mockModelSet.propertyOptions.value);
  }

  @Test
  void testSetPropertyOfModelVersionByBothAliasAndVersion() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelSet mockModelSet =
        new org.apache.gravitino.cli.handler.MockModelSet();

    picocli.CommandLine cmd = new picocli.CommandLine(root).addSubcommand("mock_set", mockModelSet);
    String[] args = {
      "mock_set",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--version=1",
      "--alias=alias1",
      "--property",
      "k1",
      "--value",
      "v2"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MutuallyExclusiveArgsException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testSetPropertyOfModel() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelSet mockModelSet =
        new org.apache.gravitino.cli.handler.MockModelSet();

    picocli.CommandLine cmd = new picocli.CommandLine(root).addSubcommand("mock_set", mockModelSet);
    String[] args = {
      "mock_set",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--property",
      "k1",
      "--value",
      "v2"
    };

    int exitCode1 = cmd.execute(args);
    Assertions.assertEquals(0, exitCode1);
    Assertions.assertEquals("ml1", mockModelSet.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelSet.name);
    Assertions.assertNull(mockModelSet.versionOrAlias);
    Assertions.assertEquals("k1", mockModelSet.propertyOptions.property);
    Assertions.assertEquals("v2", mockModelSet.propertyOptions.value);
  }

  @Test
  void testSetModelPropertyWithMalformedEntity() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelSet mockModelSet =
        new org.apache.gravitino.cli.handler.MockModelSet();

    picocli.CommandLine cmd = new picocli.CommandLine(root).addSubcommand("mock_set", mockModelSet);
    String[] args = {
      "mock_set",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1",
      "--version",
      "2",
      "--property",
      "k1",
      "--value",
      "v2"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertNotEquals(0, exitCode);
  }

  @Test
  void testSetPropertyOfModelWithoutValueOption() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelSet mockModelSet =
        new org.apache.gravitino.cli.handler.MockModelSet();

    picocli.CommandLine cmd = new picocli.CommandLine(root).addSubcommand("mock_set", mockModelSet);
    String[] args = {
      "mock_set", "--metalake", "ml1", "--name", "catalog1.schema1.model1", "--property", "k1"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testSetPropertyOfModelWithoutPropertyOption() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelSet mockModelSet =
        new org.apache.gravitino.cli.handler.MockModelSet();

    picocli.CommandLine cmd = new picocli.CommandLine(root).addSubcommand("mock_set", mockModelSet);
    String[] args = {
      "mock_set", "--metalake", "ml1", "--name", "catalog1.schema1.model1", "--value", "v1"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testRemovePropertyOfModelVersionByAlias() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {
      "mock_remove",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--alias",
      "alias1",
      "--property=k1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelRemove.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelRemove.name);
    Assertions.assertEquals("alias1", mockModelRemove.versionOrAlias.alias);
    Assertions.assertEquals("k1", mockModelRemove.removeOptions.removedProperty);
  }

  @Test
  void testRemovePropertyOfModelVersionByVersion() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {
      "mock_remove",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--version",
      "2",
      "--property=k1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelRemove.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelRemove.name);
    Assertions.assertEquals(2, mockModelRemove.versionOrAlias.version);
    Assertions.assertEquals("k1", mockModelRemove.removeOptions.removedProperty);
  }

  @Test
  void testRemoveAliasOfModelVersionByAlias() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {
      "mock_remove",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--alias",
      "alias1",
      "--removealias=alias1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelRemove.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelRemove.name);
    Assertions.assertEquals("alias1", mockModelRemove.versionOrAlias.alias);
    Assertions.assertArrayEquals(
        new String[] {"alias1"}, mockModelRemove.removeOptions.removedAlias);
  }

  @Test
  void testRemoveAliasOfModelByVersion() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {
      "mock_remove",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--version",
      "2",
      "--removealias=alias1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelRemove.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelRemove.name);
    Assertions.assertEquals(2, mockModelRemove.versionOrAlias.version);
    Assertions.assertNull(mockModelRemove.versionOrAlias.alias);
    Assertions.assertArrayEquals(
        new String[] {"alias1"}, mockModelRemove.removeOptions.removedAlias);
  }

  @Test
  void testRemovePropertyOfModel() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {
      "mock_remove", "--metalake", "ml1", "--name", "catalog1.schema1.model1", "--property", "k1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelRemove.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelRemove.name);
    Assertions.assertEquals("k1", mockModelRemove.removeOptions.removedProperty);
    Assertions.assertNull(mockModelRemove.versionOrAlias);
  }

  @Test
  void testRemoveBothPropertyAndAlias() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {
      "mock_remove",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--property",
      "k1",
      "--removealias",
      "alias1"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MutuallyExclusiveArgsException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testRemoveModelVersionSpecifyBothOfVersionAndAlias() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {
      "mock_remove",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--removealias",
      "alias1",
      "--version",
      "1",
      "--alias",
      "alias1"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MutuallyExclusiveArgsException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testRemoveModelWithoutTarget() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {"mock_remove", "--metalake", "ml1", "--name", "catalog1.schema1.model1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testRemoveModelWithMalformedEntity() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {
      "mock_remove", "--metalake", "ml1", "--name", "catalog1.schema1", "--property", "k1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertNotEquals(0, exitCode);
  }

  @Test
  void testRemoveModelVersionWithoutTarget() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelRemove mockModelRemove =
        new org.apache.gravitino.cli.handler.MockModelRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockModelRemove);
    String[] args = {
      "mock_remove", "--metalake", "ml1", "--name", "catalog1.schema1.model1", "--version=1"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testUpdateCommentOfModel() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--comment",
      "new_comment"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelUpdate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelUpdate.name);
    Assertions.assertEquals("new_comment", mockModelUpdate.comment);
  }

  @Test
  void testUpdateNameOfModel() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--rename",
      "new_name"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelUpdate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelUpdate.name);
    Assertions.assertEquals("new_name", mockModelUpdate.updateOptions.newName);
  }

  @Test
  void testUpdateCommentOfModelVersionByAlias() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--alias",
      "alias1",
      "--comment",
      "new_comment"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelUpdate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelUpdate.name);
    Assertions.assertArrayEquals(new String[] {"alias1"}, mockModelUpdate.aliases);
    Assertions.assertNotNull(mockModelUpdate.comment);
    Assertions.assertEquals("new_comment", mockModelUpdate.comment);
  }

  @Test
  void testUpdateCommentOfModelVersionByVersion() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--version",
      "2",
      "--comment",
      "new_comment"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelUpdate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelUpdate.name);
    Assertions.assertEquals(2, mockModelUpdate.version);
    Assertions.assertNotNull(mockModelUpdate.comment);
    Assertions.assertEquals("new_comment", mockModelUpdate.comment);
  }

  @Test
  void testUpdateAliasOfModelVersionByAlias() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--alias",
      "alias1",
      "--newalias",
      "alias2",
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelUpdate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelUpdate.name);
    Assertions.assertArrayEquals(new String[] {"alias1"}, mockModelUpdate.aliases);
    Assertions.assertNotNull(mockModelUpdate.updateOptions);
    Assertions.assertArrayEquals(new String[] {"alias2"}, mockModelUpdate.updateOptions.newAliases);
  }

  @Test
  void testUpdateAliasOfModelVersionByVersion() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--version",
      "2",
      "--newalias",
      "alias2",
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelUpdate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelUpdate.name);
    Assertions.assertEquals(2, mockModelUpdate.version);
    Assertions.assertNotNull(mockModelUpdate.updateOptions);
    Assertions.assertArrayEquals(new String[] {"alias2"}, mockModelUpdate.updateOptions.newAliases);
  }

  @Test
  void testUpdateUriOfModelVersionByAlias() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--alias",
      "alias1",
      "--newuri",
      "s3://new_uri",
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelUpdate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelUpdate.name);
    Assertions.assertArrayEquals(new String[] {"alias1"}, mockModelUpdate.aliases);
    Assertions.assertNotNull(mockModelUpdate.updateOptions);
    Assertions.assertNotNull(mockModelUpdate.updateOptions.newUri);
    Assertions.assertEquals("s3://new_uri", mockModelUpdate.updateOptions.newUri);
  }

  @Test
  void testUpdateUriOfModelVersionByVersion() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--version",
      "2",
      "--newuri",
      "s3://new_uri",
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelUpdate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelUpdate.name);
    Assertions.assertEquals(2, mockModelUpdate.version);
    Assertions.assertNotNull(mockModelUpdate.updateOptions);
    Assertions.assertNotNull(mockModelUpdate.updateOptions.newUri);
    Assertions.assertEquals("s3://new_uri", mockModelUpdate.updateOptions.newUri);
  }

  @Test
  void testLinkModel() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1.model1",
      "--uris",
      "n1=u1,n2=u2",
      "--alias",
      "aliasA",
      "--alias",
      "aliasB"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockModelUpdate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1.model1", mockModelUpdate.name);
    Assertions.assertArrayEquals(new String[] {"aliasA", "aliasB"}, mockModelUpdate.aliases);
    Assertions.assertEquals("u1", mockModelUpdate.updateOptions.uris.get("n1"));
    Assertions.assertEquals("u2", mockModelUpdate.updateOptions.uris.get("n2"));
  }

  @Test
  void testUpdateModelWithMalformedEntity() {
    ModelCliHandler root = new ModelCliHandler();
    org.apache.gravitino.cli.handler.MockModelUpdate mockModelUpdate =
        new org.apache.gravitino.cli.handler.MockModelUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockModelUpdate);
    String[] args = {
      "mock_update",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1",
      "--uris",
      "n1=u1,n2=u2",
      "--alias",
      "aliasA",
      "--alias",
      "aliasB"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertNotEquals(0, exitCode);
  }
}

/** Mock classes for testing Model create command. */
@picocli.CommandLine.Command(name = "mock_create", description = "Create a new model")
class MockModelCreate extends ModelCreate {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock classes for testing Model details command. */
@picocli.CommandLine.Command(name = "mock_details", description = "Create a new model")
class MockModelDetails extends ModelDetails {

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock classes for testing Model list command. */
@picocli.CommandLine.Command(
    name = "mock_list",
    description = "List models in specified metalake.catalog.schema")
class MockModeList extends ModelList {

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock classes for testing Model delete command. */
@picocli.CommandLine.Command(
    name = "mock_delete",
    description = "Delete model in specified metalake.catalog.schema")
class MockModelDelete extends ModelDelete {

  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock classes for testing Model set command. */
@picocli.CommandLine.Command(
    name = "mock_set",
    description = "Set property of a model version by alias or version")
class MockModelSet extends ModelSet {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock class for testing model remove command. */
@picocli.CommandLine.Command(
    name = CommandActions.REMOVE,
    description = "Remove property of a model version by alias or version")
class MockModelRemove extends ModelRemove {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock class for testing model update command. */
@CommandLine.Command(
    name = "mock_update",
    usageHelpAutoWidth = true,
    description = "Update property of model or model version, like name, comment, alias")
class MockModelUpdate extends ModelUpdate {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}
