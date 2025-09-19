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

class TestMetalakeCommands {
  @Test
  void testHelpCommandOfMetalake() {
    String[] args1 = {CommandEntities.METALAKE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(args1));

    String[] args2 = {CommandEntities.METALAKE};
    Assertions.assertDoesNotThrow(() -> MainCli.main(args2));
  }

  @Test
  void testPrintMetalakeCommandHelp() {
    String[] detailsArgs = {CommandEntities.METALAKE, CommandActions.DETAILS, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(detailsArgs));

    String[] createArgs = {CommandEntities.METALAKE, CommandActions.CREATE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(createArgs));

    String[] createArgs2 = {
      CommandEntities.METALAKE, CommandActions.CREATE, "--help", "--metalake", "ml1"
    };
    Assertions.assertDoesNotThrow(() -> MainCli.main(createArgs2));

    String[] deleteArgs = {CommandEntities.METALAKE, CommandActions.DELETE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(deleteArgs));

    String[] setArgs = {CommandEntities.METALAKE, CommandActions.SET, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(setArgs));

    String[] removeArgs = {CommandEntities.METALAKE, CommandActions.REMOVE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(removeArgs));

    String[] propertiesArgs = {CommandEntities.METALAKE, CommandActions.PROPERTIES, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(propertiesArgs));

    String[] updateArgs = {CommandEntities.METALAKE, CommandActions.UPDATE, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(updateArgs));

    String[] listArgs = {CommandEntities.METALAKE, CommandActions.LIST, "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(listArgs));
  }

  @Test
  void testGetDetailsOfMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeDetails mockMetalakeDetails =
        new org.apache.gravitino.cli.handler.MockMetalakeDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockMetalakeDetails);
    String[] args = {"mock_details", "--metalake", "ml1", "--output", "TABLE", "--no-audit"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeDetails.commonOptions.metalake);
    Assertions.assertEquals(
        OutputFormat.OutputType.TABLE, mockMetalakeDetails.commonOptions.outputFormat);
    Assertions.assertFalse(mockMetalakeDetails.audit);
  }

  @Test
  void testGetAuditOfMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeDetails mockMetalakeAudit =
        new org.apache.gravitino.cli.handler.MockMetalakeDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_audit", mockMetalakeAudit);
    String[] args = {"mock_audit", "--metalake", "ml1", "--output", "TABLE", "--audit"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeAudit.commonOptions.metalake);
    Assertions.assertEquals(
        OutputFormat.OutputType.TABLE, mockMetalakeAudit.commonOptions.outputFormat);
    Assertions.assertTrue(mockMetalakeAudit.audit);
  }

  @Test
  void testGetDetailsOfMetalakeWithDefaults() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeDetails mockMetalakeDetails =
        new org.apache.gravitino.cli.handler.MockMetalakeDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockMetalakeDetails);
    String[] args = {"mock_details", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeDetails.commonOptions.metalake);
    Assertions.assertEquals(
        OutputFormat.OutputType.PLAIN, mockMetalakeDetails.commonOptions.outputFormat);
    Assertions.assertFalse(mockMetalakeDetails.audit);
  }

  @Test
  void testGetDetailsOfMetalakeWithoutMetalake() {
    MainCli.useExit = false;
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeDetails mockMetalakeDetails =
        new org.apache.gravitino.cli.handler.MockMetalakeDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockMetalakeDetails);
    String[] args = {"mock_details"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testCreateMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeCreate mockMetalakeCreate =
        new org.apache.gravitino.cli.handler.MockMetalakeCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockMetalakeCreate);
    String[] args = {"mock_create", "--metalake", "ml1", "--comment", "comment1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeCreate.commonOptions.metalake);
    Assertions.assertEquals("comment1", mockMetalakeCreate.comment);
  }

  @Test
  void testCreateMetalakeWithDefaults() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeCreate mockMetalakeCreate =
        new org.apache.gravitino.cli.handler.MockMetalakeCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockMetalakeCreate);
    String[] args = {"mock_create", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeCreate.commonOptions.metalake);
    Assertions.assertEquals("", mockMetalakeCreate.comment);
  }

  @Test
  void testCreateMetalakeWithoutMetalake() {
    MainCli.useExit = false;
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeCreate mockMetalakeCreate =
        new org.apache.gravitino.cli.handler.MockMetalakeCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockMetalakeCreate);
    String[] args = {"mock_create"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testDeleteMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeDelete mockMetalakeDelete =
        new org.apache.gravitino.cli.handler.MockMetalakeDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockMetalakeDelete);
    String[] args = {"mock_delete", "--metalake", "ml1", "--force"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeDelete.commonOptions.metalake);
    Assertions.assertTrue(mockMetalakeDelete.force);
  }

  @Test
  void testDeleteMetalakeWithoutMetalake() {
    MainCli.useExit = false;
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeDelete mockMetalakeDelete =
        new org.apache.gravitino.cli.handler.MockMetalakeDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockMetalakeDelete);
    String[] args = {"mock_delete"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testDeleteMetalakeWithDefaults() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeDelete mockMetalakeDelete =
        new org.apache.gravitino.cli.handler.MockMetalakeDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockMetalakeDelete);
    String[] args = {"mock_delete", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeDelete.commonOptions.metalake);
    Assertions.assertFalse(mockMetalakeDelete.force);
  }

  @Test
  void testSetMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeSet mockMetalakeSet =
        new org.apache.gravitino.cli.handler.MockMetalakeSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockMetalakeSet);
    String[] args = {"mock_set", "--metalake", "ml1", "--property", "key1", "--value", "value1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeSet.commonOptions.metalake);
    Assertions.assertEquals("key1", mockMetalakeSet.propertyOptions.property);
    Assertions.assertEquals("value1", mockMetalakeSet.propertyOptions.value);
  }

  @Test
  void testSetMetalakeWithoutMetalake() {
    MainCli.useExit = false;
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeSet mockMetalakeSet =
        new org.apache.gravitino.cli.handler.MockMetalakeSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockMetalakeSet);
    String[] args = {"mock_set", "--property", "key1", "--value", "value1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testSetMetalakeWithoutProperty() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeSet mockMetalakeSet =
        new org.apache.gravitino.cli.handler.MockMetalakeSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockMetalakeSet);
    String[] args = {"mock_set", "--metalake", "ml1", "--value", "value1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testSetMetalakeWithoutValue() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeSet mockMetalakeSet =
        new org.apache.gravitino.cli.handler.MockMetalakeSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockMetalakeSet);
    String[] args = {"mock_set", "--metalake", "ml1", "--property", "key1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testRemoveMetalakeProperty() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeRemove mockMetalakeRemove =
        new org.apache.gravitino.cli.handler.MockMetalakeRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockMetalakeRemove);
    String[] args = {"mock_remove", "--metalake", "ml1", "--property", "key1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeRemove.commonOptions.metalake);
    Assertions.assertEquals("key1", mockMetalakeRemove.property);
  }

  @Test
  void testRemoveMetalakePropertyWithoutMetalake() {
    MainCli.useExit = false;
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeRemove mockMetalakeRemove =
        new org.apache.gravitino.cli.handler.MockMetalakeRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockMetalakeRemove);
    String[] args = {"mock_remove", "--property", "key1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testRemoveMetalakePropertyWithoutProperty() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeRemove mockMetalakeRemove =
        new org.apache.gravitino.cli.handler.MockMetalakeRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockMetalakeRemove);
    String[] args = {"mock_remove", "--metalake", "ml1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testListPropertiesOfMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeProperties mockMetalakeProperties =
        new org.apache.gravitino.cli.handler.MockMetalakeProperties();
    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_properties", mockMetalakeProperties);
    String[] args = {"mock_properties", "--metalake", "ml1", "--output", "TABLE"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeProperties.commonOptions.metalake);
    Assertions.assertEquals(
        OutputFormat.OutputType.TABLE, mockMetalakeProperties.commonOptions.outputFormat);
  }

  @Test
  void testListPropertiesOfMetalakeWithoutMetalake() {
    MainCli.useExit = false;
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeProperties mockMetalakeProperties =
        new org.apache.gravitino.cli.handler.MockMetalakeProperties();
    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_properties", mockMetalakeProperties);
    String[] args = {"mock_properties"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testListPropertiesOfMetalakeWithDefaults() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeProperties mockMetalakeProperties =
        new org.apache.gravitino.cli.handler.MockMetalakeProperties();
    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_properties", mockMetalakeProperties);
    String[] args = {"mock_properties", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeProperties.commonOptions.metalake);
    Assertions.assertEquals(
        OutputFormat.OutputType.PLAIN, mockMetalakeProperties.commonOptions.outputFormat);
  }

  @Test
  void testListMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeList mockMetalakeList =
        new org.apache.gravitino.cli.handler.MockMetalakeList();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_list", mockMetalakeList);
    String[] args = {"mock_list", "--output", "TABLE", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals(
        OutputFormat.OutputType.TABLE, mockMetalakeList.commonOptions.outputFormat);
    Assertions.assertEquals("ml1", mockMetalakeList.commonOptions.metalake);
  }

  @Test
  void testListMetalakeWithoutMetalake() {
    MainCli.useExit = false;
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeList mockMetalakeList =
        new org.apache.gravitino.cli.handler.MockMetalakeList();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_list", mockMetalakeList);
    String[] args = {"mock_list"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testListMetalakeWithDefaults() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeList mockMetalakeList =
        new org.apache.gravitino.cli.handler.MockMetalakeList();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_list", mockMetalakeList);
    String[] args = {"mock_list", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals(
        OutputFormat.OutputType.PLAIN, mockMetalakeList.commonOptions.outputFormat);
    Assertions.assertEquals("ml1", mockMetalakeList.commonOptions.metalake);
  }

  @Test
  void testEnableMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeUpdate mockMetalakeUpdate =
        new org.apache.gravitino.cli.handler.MockMetalakeUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockMetalakeUpdate);
    String[] args = {"mock_update", "--metalake", "ml1", "--enable"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeUpdate.commonOptions.metalake);
    Assertions.assertTrue(mockMetalakeUpdate.updateOptions.enableDisableOptions.enable);
  }

  @Test
  void testDisableMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeUpdate mockMetalakeUpdate =
        new org.apache.gravitino.cli.handler.MockMetalakeUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockMetalakeUpdate);
    String[] args = {"mock_update", "--metalake", "ml1", "--disable"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeUpdate.commonOptions.metalake);
    Assertions.assertFalse(mockMetalakeUpdate.updateOptions.enableDisableOptions.enable);
  }

  @Test
  void testEnableAndDisableMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeUpdate mockMetalakeUpdate =
        new org.apache.gravitino.cli.handler.MockMetalakeUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockMetalakeUpdate);
    String[] args = {"mock_update", "--metalake", "ml1", "--enable", "--disable"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MaxValuesExceededException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testUpdateNameOfMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeUpdate mockMetalakeUpdate =
        new org.apache.gravitino.cli.handler.MockMetalakeUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockMetalakeUpdate);
    String[] args = {"mock_update", "--metalake", "ml1", "--rename", "newName"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeUpdate.commonOptions.metalake);
    Assertions.assertEquals("newName", mockMetalakeUpdate.updateOptions.newName);
  }

  @Test
  void testUpdateCommentOfMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeUpdate mockMetalakeUpdate =
        new org.apache.gravitino.cli.handler.MockMetalakeUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockMetalakeUpdate);
    String[] args = {"mock_update", "--metalake", "ml1", "--comment", "newComment"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockMetalakeUpdate.commonOptions.metalake);
    Assertions.assertEquals("newComment", mockMetalakeUpdate.updateOptions.comment);
  }

  @Test
  void testUpdateBothNameAndCommentOfMetalake() {
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeUpdate mockMetalakeUpdate =
        new org.apache.gravitino.cli.handler.MockMetalakeUpdate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_update", mockMetalakeUpdate);
    String[] args = {
      "mock_update", "--metalake", "ml1", "--rename", "newName", "--comment", "newComment"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MutuallyExclusiveArgsException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testUpdateWithoutMetalake() {
    MainCli.useExit = false;
    MetalakeCliHandler root = new MetalakeCliHandler();
    org.apache.gravitino.cli.handler.MockMetalakeUpdate mockMetalakeUpdate =
        new org.apache.gravitino.cli.handler.MockMetalakeUpdate();

    picocli.CommandLine cmd =
        new CommandLine(root).addSubcommand("mock_update", mockMetalakeUpdate);
    String[] args = {"mock_update", "--comment", "new_comment"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }
}

/** Mock for MetalakeDetails. */
class MockMetalakeDetails extends MetalakeDetails {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock for MetalakeCreate. */
class MockMetalakeCreate extends MetalakeCreate {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock for MetalakeDelete. */
class MockMetalakeDelete extends MetalakeDelete {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock for MetalakeRemove. */
class MockMetalakeSet extends MetalakeSet {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock for MetalakeProperties. */
class MockMetalakeRemove extends MetalakeRemove {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock for MetalakeProperties. */
class MockMetalakeProperties extends MetalakeProperties {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock for MetalakeList. */
class MockMetalakeList extends MetalakeList {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** Mock for MetalakeUpdate. */
class MockMetalakeUpdate extends MetalakeUpdate {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}
