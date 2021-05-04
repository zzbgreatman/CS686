package edu.usfca.dataflow.utils;

import edu.usfca.dataflow.__TestBase;
import edu.usfca.dataflow.utils.GeneralUtils.AppComparator;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Common.SalesEvent;
import edu.usfca.protobuf.Common.SalesSummary.App;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class __TestGeneralUtils {
  // Note: This rule is used to stop the grading system from going down when your code gets stuck (safety measure).
  // If you think this prevents your submission from getting graded normally, ask on Piazza.
  @Rule public Timeout timeout = Timeout.seconds(1);

  @Test public void testGetNormalized() {
    SalesEvent.Builder se = SalesEvent.newBuilder();
    DeviceId.Builder id = DeviceId.newBuilder();

    id.setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toUpperCase());
    se.setId(id);

    SalesEvent normalized = GeneralUtils.getNormalized(se.build());
    assertEquals(OsType.OS_ANDROID, normalized.getId().getOs());
    assertEquals(__TestBase.UUID1.toLowerCase(), normalized.getId().getUuid().toLowerCase());
    assertEquals("", normalized.getId().getWebid());

    id.setOs(OsType.OS_UNSPECIFIED).setWebid("lower");
    se.setId(id);
    normalized = GeneralUtils.getNormalized(se.build());
    assertEquals(OsType.OS_UNSPECIFIED, normalized.getId().getOs());
    assertEquals("", normalized.getId().getUuid());
    assertEquals("lower", normalized.getId().getWebid());

    id.setOs(OsType.OS_IOS).setWebid("UPPER");
    se.setId(id);
    normalized = GeneralUtils.getNormalized(se.build());
    assertEquals(OsType.OS_IOS, normalized.getId().getOs());
    assertEquals("", normalized.getId().getUuid());
    assertEquals("UPPER", normalized.getId().getWebid());
  }


  @Test public void testAppComparator() {
    App app1 = App.newBuilder().setAmount(100).setCntUsers(150).setBundle("app").build();
    App app2 = App.newBuilder().setAmount(100).setCntUsers(100).setBundle("APP").build();
    App app3 = App.newBuilder().setAmount(100).setCntUsers(150).setBundle("APP").build();
    App app4 = App.newBuilder().setAmount(200).setCntUsers(100).setBundle("xyz").build();
    App app5 = App.newBuilder().setAmount(200).setCntUsers(150).setBundle("XY").build();
    App app6 = App.newBuilder().setAmount(200).setCntUsers(150).setBundle("XYZ").build();

    {
      // Default behavior is to sort apps in descending order of sales amount.
      List<App> sortedApps = Arrays.asList(app1, app2, app3, app4, app5, app6).stream()//
        .sorted(new AppComparator()).collect(Collectors.toList());

      assertEquals(app5, sortedApps.get(0));
      assertEquals(app6, sortedApps.get(1));
      assertEquals(app4, sortedApps.get(2));
      assertEquals(app3, sortedApps.get(3));
      assertEquals(app1, sortedApps.get(4));
      assertEquals(app2, sortedApps.get(5));

      System.out.format("%s\n", Arrays.toString(sortedApps.toArray()));
    }

    {
      // We can optionally specify 'true' which is the default behavior.
      List<App> sortedApps = Arrays.asList(app1, app2, app3, app4, app5, app6).stream()//
        .sorted(new AppComparator(true)).collect(Collectors.toList());

      assertEquals(app5, sortedApps.get(0));
      assertEquals(app6, sortedApps.get(1));
      assertEquals(app4, sortedApps.get(2));
      assertEquals(app3, sortedApps.get(3));
      assertEquals(app1, sortedApps.get(4));
      assertEquals(app2, sortedApps.get(5));

      System.out.format("%s\n", Arrays.toString(sortedApps.toArray()));
    }


    {
      // We can optionally specify 'false' which is to sort in ascending order.
      // Note that you need to take ties into account.
      List<App> sortedApps = Arrays.asList(app1, app2, app3, app4, app5, app6).stream()//
        .sorted(new AppComparator(false)).collect(Collectors.toList());

      assertEquals(app5, sortedApps.get(5));
      assertEquals(app6, sortedApps.get(4));
      assertEquals(app4, sortedApps.get(3));
      assertEquals(app3, sortedApps.get(2));
      assertEquals(app1, sortedApps.get(1));
      assertEquals(app2, sortedApps.get(0));

      System.out.format("%s\n", Arrays.toString(sortedApps.toArray()));
    }
  }
}
