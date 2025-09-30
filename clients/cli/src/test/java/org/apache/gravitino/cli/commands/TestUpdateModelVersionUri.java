package org.apache.gravitino.cli.commands;

import static org.mockito.Mockito.spy;

import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelVersionChange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class TestUpdateModelVersionUri {

  @BeforeEach
  void setUp() {
    Main.useExit = false;
  }

  @AfterEach
  void tearDown() {
    Main.useExit = true;
  }

  @Test
  void handleWithAliasCallsAlterModelVersion() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionUri command =
        spy(
            new UpdateModelVersionUri(
                context,
                "metalake1",
                "catalog1",
                "schema1",
                "model1",
                null,
                "alias1",
                "http://some.uri"));

    GravitinoClient mockClient = Mockito.mock(GravitinoClient.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    ModelCatalog mockModelCatalog = Mockito.mock(ModelCatalog.class);

    Mockito.doReturn(mockClient).when(command).buildClient("metalake1");
    Mockito.when(mockClient.loadCatalog("catalog1")).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.asModelCatalog()).thenReturn(mockModelCatalog);

    command.handle();

    Mockito.verify(mockModelCatalog)
        .alterModelVersion(
            ArgumentMatchers.any(NameIdentifier.class),
            ArgumentMatchers.eq("alias1"),
            ArgumentMatchers.any(ModelVersionChange.class));
    Mockito.verify(command)
        .printInformation(ArgumentMatchers.contains("alias alias1 uri changed."));
  }

  @Test
  void handleWithVersionCallsAlterModelVersion() throws Exception {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionUri command =
        spy(
            new UpdateModelVersionUri(
                context, "metalake1", "catalog1", "schema1", "model1", 4, null, "http://some.uri"));

    GravitinoClient mockClient = Mockito.mock(GravitinoClient.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    ModelCatalog mockModelCatalog = Mockito.mock(ModelCatalog.class);

    Mockito.doReturn(mockClient).when(command).buildClient("metalake1");
    Mockito.when(mockClient.loadCatalog("catalog1")).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.asModelCatalog()).thenReturn(mockModelCatalog);

    command.handle();

    Mockito.verify(mockModelCatalog)
        .alterModelVersion(
            ArgumentMatchers.any(NameIdentifier.class),
            ArgumentMatchers.eq(4),
            ArgumentMatchers.any(ModelVersionChange.class));
    Mockito.verify(command).printInformation(ArgumentMatchers.contains("version 4 uri changed."));
  }

  @Test
  void validateBothAliasAndVersionNotNullShouldExitWithError() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionUri command =
        new UpdateModelVersionUri(
            context, "metalake1", "catalog1", "schema1", "model1", 1, "alias1", "http://new.uri");

    RuntimeException ex = Assertions.assertThrows(RuntimeException.class, command::validate);
    Assertions.assertTrue(ex.getMessage().contains("Exit with code"));
  }

  @Test
  void validateBothAliasAndVersionNullShouldExitWithError() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionUri command =
        new UpdateModelVersionUri(
            context, "metalake1", "catalog1", "schema1", "model1", null, null, "http://new.uri");

    RuntimeException ex = Assertions.assertThrows(RuntimeException.class, command::validate);
    Assertions.assertTrue(ex.getMessage().contains("Exit with code"));
  }

  @Test
  void validateOnlyAliasSetShouldPass() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionUri command =
        new UpdateModelVersionUri(
            context,
            "metalake1",
            "catalog1",
            "schema1",
            "model1",
            null,
            "alias1",
            "http://new.uri");

    Assertions.assertDoesNotThrow(command::validate);
  }

  @Test
  void validateOnlyVersionSetShouldPass() {
    CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
    CommandContext context = new CommandContext(mockCmdLine);

    UpdateModelVersionUri command =
        new UpdateModelVersionUri(
            context, "metalake1", "catalog1", "schema1", "model1", 1, null, "http://new.uri");

    Assertions.assertDoesNotThrow(command::validate);
  }
}
