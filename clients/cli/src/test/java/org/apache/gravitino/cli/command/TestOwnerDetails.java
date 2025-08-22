import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import com.github.stefanbirkner.systemrules.ExpectedSystemExit;
import org.apache.gravitino.cli.command.Command;
import org.apache.gravitino.cli.command.CommandContext;
import org.apache.gravitino.cli.command.CommandEntities;
import org.apache.gravitino.cli.command.OwnerDetails;
import org.junit.Rule;
import org.junit.Test;

public class TestOwnerDetails {

  @Rule public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Test
  public void testValidate_withValidEntityType() {
    CommandContext context = new CommandContext();
    OwnerDetails details = new OwnerDetails(context, "metalake1", "entity1", CommandEntities.TABLE);

    Command result = details.validate();

    assertNotNull(result);
    assertSame(details, result);
  }

  @Test
  public void testValidate_withInvalidEntityType_shouldExit() {
    CommandContext context = new CommandContext();
    OwnerDetails details = new OwnerDetails(context, "metalake1", "entity1", "INVALID_TYPE");

    exit.expectSystemExitWithStatus(-1);

    details.validate(); // triggers exit(-1)
  }
}
