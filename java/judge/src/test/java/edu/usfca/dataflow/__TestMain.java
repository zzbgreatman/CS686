package edu.usfca.dataflow;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;

public class __TestMain {
  // Note: This rule is used to stop the grading system from going down when your code gets stuck (safety measure).
  // If you think this prevents your submission from getting graded normally, ask on Piazza.
  @Rule public Timeout timeout = Timeout.seconds(1);

  @Test public void testGetUserEmail() {
    assertTrue(StringUtils.endsWith(Main.getUserEmail(), "usfca.edu"));
  }
}
