package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.Namespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ListTopicFailureEventTest {

  @Test
  public void testNamespaceMustNotBeNull() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ListTopicFailureEvent("user", null, new Exception("boom")));
  }
}
