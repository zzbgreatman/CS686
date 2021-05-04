package edu.usfca.dataflow.utils;

import edu.usfca.protobuf.Common.OsType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


// NOTE: If your IDE complains about the protobuf imports above,
// run `gradle test` under <your repo>/java directory to generate Java files from proto files.
// Then, reload your IDE or follow the instructions from your gradle plugin (for intelliJ or Eclipse).


public class TestProtoUtils {
  // TODO: To add your own unit tests.

  @Test public void testDummy() {
    // Feel free to remove this dummy unit test.
    OsType testVariable = OsType.OS_ANDROID;
    assertEquals(OsType.OS_ANDROID, testVariable);
    assertNotEquals(OsType.OS_IOS, testVariable);
  }
}
