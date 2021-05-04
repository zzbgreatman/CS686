package edu.usfca.dataflow.transforms;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import edu.usfca.dataflow.__TestBase;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Common.SalesEvent;
import edu.usfca.protobuf.Common.SalesSummary;
import edu.usfca.protobuf.Common.SalesSummary.App;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class __TestComputeSummary implements Serializable {
  // This is to help the grading system avoid 'hanging forever'
  // If unit tests keep failing due to 'timeout' on your local machine or when you need to use a debugger,
  // you should comment this out (this is only necessary for the grading system).
  @Rule public Timeout timeout = Timeout.seconds(15);


  @Rule public final transient TestPipeline tp = TestPipeline.create();

  @Before public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  // -------------------------

  @Test public void testSample00() {
    long ts = Instant.parse("2021-05-01T10:15:15.000Z").toEpochMilli(); // 1619864115000
    System.out.format("%d\n", ts);
    assertEquals(1619864115000L, ts);

    SalesEvent.Builder se = SalesEvent.newBuilder();
    DeviceId id1 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id2 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id3 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id4 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id5 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toUpperCase()).build();
    DeviceId id6 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toLowerCase()).build();

    // Notice that there is only one app in this dataset.
    List<SalesEvent> inputData = Arrays.asList(//
      se.setId(id1).setAmount(10).setBundle("app-g").build(),//
      se.setId(id2).setAmount(50).setBundle("app-g").build(),//
      se.setId(id3).setAmount(50).setBundle("app-g").build(),//
      se.setId(id4).setAmount(50).setBundle("app-g").build(),//
      se.setId(id5).setAmount(50).setBundle("app-g").build(),//
      se.setId(id6).setAmount(50).setBundle("app-g").build()//
    );

    PCollection<String> input = tp.apply(Create.of(String.format("%d %s", ts,
      inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" ")))));

    PCollection<SalesSummary> output = input.apply(new Parser()).apply(new ComputeSummary(5));

    PAssert.that(output).satisfies(out -> {
      assertEquals(30, Iterables.size(out));
      boolean found = false;
      Iterator<SalesSummary> it = out.iterator();
      while (it.hasNext()) {
        SalesSummary ss = it.next();
        if ("10:19:59".equals(ss.getWindowEnd())) {
          assertFalse(found);
          found = true;

          assertEquals("10:19:59", ss.getWindowEnd());
          assertEquals(1, ss.getTopAppsCount());
          App app1 = ss.getTopApps(0);

          assertEquals("app-g", app1.getBundle());
          assertEquals(260, app1.getAmount());
          assertEquals(4, app1.getCntUsers());
        }
      }
      assertTrue(found);

      return null;
    });

    tp.run();
  }

  @Test public void testSample01() {
    long ts = Instant.parse("2021-05-01T10:15:15.000Z").toEpochMilli(); // 1619864115000
    System.out.format("%d\n", ts);
    assertEquals(1619864115000L, ts);

    SalesEvent.Builder se = SalesEvent.newBuilder();
    DeviceId id1 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id2 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id3 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id4 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id5 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toUpperCase()).build();
    DeviceId id6 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toLowerCase()).build();

    // all of the following events take place at the same time (ts).
    // the expected output should contain one SalesSummary (corresponding to the window 10:15:00.000 - 10:19:59.999).
    // This message should contain top 3 apps sorted by amount DESC, cnt_users DESC, bundle ASC.
    // Note - for simplicity, this unit test only checks the contents of the window ending on the minute,
    // but it also checks how many windows (SalesSummary objects) your code outputs.
    List<SalesEvent> inputData = Arrays.asList(//
      se.setId(id1).setAmount(100).setBundle("app-x").build(),//
      se.setId(id2).setAmount(200).setBundle("app-x").build(),//
      se.setId(id3).setAmount(300).setBundle("app-x").build(),//
      se.setId(id4).setAmount(300).setBundle("app-b").build(),//
      se.setId(id5).setAmount(300).setBundle("app-b").build(),//
      se.setId(id6).setAmount(600).setBundle("app-a").build(),//
      se.setId(id1).setAmount(10).setBundle("app-g").build(),//
      se.setId(id2).setAmount(50).setBundle("app-g").build(),//
      se.setId(id3).setAmount(50).setBundle("app-g").build(),//
      se.setId(id4).setAmount(50).setBundle("app-g").build(),//
      se.setId(id5).setAmount(50).setBundle("app-g").build(),//
      se.setId(id6).setAmount(50).setBundle("app-g").build()//
    );

    PCollection<String> input = tp.apply(Create.of(String.format("%d %s", ts,
      inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" ")))));

    PCollection<SalesSummary> output = input.apply(new Parser()).apply(new ComputeSummary(5));

    PAssert.that(output).satisfies(out -> {
      assertEquals(30, Iterables.size(out));
      boolean found = false;
      Iterator<SalesSummary> it = out.iterator();
      while (it.hasNext()) {
        SalesSummary ss = it.next();
        if ("10:19:59".equals(ss.getWindowEnd())) {
          assertFalse(found);
          found = true;

          assertEquals("10:19:59", ss.getWindowEnd());
          assertEquals(3, ss.getTopAppsCount());
          App app1 = ss.getTopApps(0), app2 = ss.getTopApps(1), app3 = ss.getTopApps(2);

          assertEquals("app-b", app1.getBundle());
          assertEquals(600, app1.getAmount());
          assertEquals(2, app1.getCntUsers());

          assertEquals("app-x", app2.getBundle());
          assertEquals(600, app2.getAmount());
          assertEquals(2, app2.getCntUsers());

          assertEquals("app-a", app3.getBundle());
          assertEquals(600, app3.getAmount());
          assertEquals(1, app3.getCntUsers());
        }
      }
      assertTrue(found);

      return null;
    });

    tp.run();
  }

  @Test public void testSample02() {
    long ts1 = Instant.parse("2021-05-01T10:15:15.000Z").toEpochMilli(); // 1619864115000
    assertEquals(1619864115000L, ts1);
    long ts2 = Instant.parse("2021-05-01T10:20:15.000Z").toEpochMilli(); // 1619864415000
    assertEquals(1619864415000L, ts2);

    SalesEvent.Builder se = SalesEvent.newBuilder();
    DeviceId id1 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id2 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id3 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id4 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id5 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toUpperCase()).build();
    DeviceId id6 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toLowerCase()).build();

    // all of the following events take place at the same time, once at ts1 and once at ts2.
    // the expected output should contain two SalesSummary messages
    // (corresponding to the window 10:15:00.000 - 10:19:59.999 & 10:20:00.000 - 10:24:59.999).
    // Each SalesSummary message should contain top 3 apps sorted by amount DESC, cnt_users DESC, bundle ASC.
    // Note - for simplicity, this unit test only checks the contents of the window ending on the minute,
    // but it also checks how many windows (SalesSummary objects) your code outputs.
    List<SalesEvent> inputData = Arrays.asList(//
      se.setId(id1).setAmount(100).setBundle("app-x").build(),//
      se.setId(id2).setAmount(200).setBundle("app-x").build(),//
      se.setId(id3).setAmount(300).setBundle("app-x").build(),//
      se.setId(id4).setAmount(300).setBundle("app-b").build(),//
      se.setId(id5).setAmount(300).setBundle("app-b").build(),//
      se.setId(id6).setAmount(600).setBundle("app-a").build(),//
      se.setId(id1).setAmount(10).setBundle("app-g").build(),//
      se.setId(id2).setAmount(50).setBundle("app-g").build(),//
      se.setId(id3).setAmount(50).setBundle("app-g").build(),//
      se.setId(id4).setAmount(50).setBundle("app-g").build(),//
      se.setId(id5).setAmount(50).setBundle("app-g").build(),//
      se.setId(id6).setAmount(50).setBundle("app-g").build()//
    );

    PCollection<String> input = tp.apply(Create.of(String.format("%d %s", ts1,
      inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" "))), String
      .format("%d %s", ts2,
        inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" ")))));

    PCollection<SalesSummary> output = input.apply(new Parser()).apply(new ComputeSummary(5));

    PAssert.that(output).satisfies(out -> {
      assertEquals(60, Iterables.size(out));
      Set<String> expected = new ImmutableSet.Builder<String>().add("10:19:59", "10:24:59").build();
      Set<String> actual = new HashSet<>();

      Iterator<SalesSummary> it = out.iterator();
      while (it.hasNext()) {
        SalesSummary ss = it.next();
        if (expected.contains(ss.getWindowEnd())) {
          assertFalse(actual.contains(ss.getWindowEnd()));
          actual.add(ss.getWindowEnd());

          assertEquals(3, ss.getTopAppsCount());
          App app1 = ss.getTopApps(0), app2 = ss.getTopApps(1), app3 = ss.getTopApps(2);

          assertEquals("app-b", app1.getBundle());
          assertEquals(600, app1.getAmount());
          assertEquals(2, app1.getCntUsers());

          assertEquals("app-x", app2.getBundle());
          assertEquals(600, app2.getAmount());
          assertEquals(2, app2.getCntUsers());

          assertEquals("app-a", app3.getBundle());
          assertEquals(600, app3.getAmount());
          assertEquals(1, app3.getCntUsers());
        }
      }
      assertEquals(expected, actual);
      return null;
    });

    tp.run();
  }

  @Test public void testSample03() {
    // same as testSample02 but with window size 1 minute each.
    long ts1 = Instant.parse("2021-05-01T10:15:15.000Z").toEpochMilli(); // 1619864115000
    assertEquals(1619864115000L, ts1);
    long ts2 = Instant.parse("2021-05-01T10:20:15.000Z").toEpochMilli(); // 1619864415000
    assertEquals(1619864415000L, ts2);

    SalesEvent.Builder se = SalesEvent.newBuilder();
    DeviceId id1 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id2 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id3 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id4 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id5 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toUpperCase()).build();
    DeviceId id6 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toLowerCase()).build();

    // all of the following events take place at the same time, once at ts1 and once at ts2.
    // the expected output should contain two SalesSummary messages
    // (corresponding to the window 10:15:00.000 - 10:15:59.999 & 10:20:00.000 - 10:20:59.999).
    // Each SalesSummary message should contain top 3 apps sorted by amount DESC, cnt_users DESC, bundle ASC.
    // Note - for simplicity, this unit test only checks the contents of the window ending on the minute,
    // but it also checks how many windows (SalesSummary objects) your code outputs.
    List<SalesEvent> inputData = Arrays.asList(//
      se.setId(id1).setAmount(100).setBundle("app-x").build(),//
      se.setId(id2).setAmount(200).setBundle("app-x").build(),//
      se.setId(id3).setAmount(300).setBundle("app-x").build(),//
      se.setId(id4).setAmount(300).setBundle("app-b").build(),//
      se.setId(id5).setAmount(300).setBundle("app-b").build(),//
      se.setId(id6).setAmount(600).setBundle("app-a").build(),//
      se.setId(id1).setAmount(10).setBundle("app-g").build(),//
      se.setId(id2).setAmount(50).setBundle("app-g").build(),//
      se.setId(id3).setAmount(50).setBundle("app-g").build(),//
      se.setId(id4).setAmount(50).setBundle("app-g").build(),//
      se.setId(id5).setAmount(50).setBundle("app-g").build(),//
      se.setId(id6).setAmount(50).setBundle("app-g").build()//
    );

    PCollection<String> input = tp.apply(Create.of(String.format("%d %s", ts1,
      inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" "))), String
      .format("%d %s", ts2,
        inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" ")))));

    PCollection<SalesSummary> output = input.apply(new Parser()).apply(new ComputeSummary(1));

    PAssert.that(output).satisfies(out -> {
      assertEquals(12, Iterables.size(out));
      Set<String> expected = new ImmutableSet.Builder<String>().add("10:15:59", "10:20:59").build();
      Set<String> actual = new HashSet<>();

      Iterator<SalesSummary> it = out.iterator();
      while (it.hasNext()) {
        SalesSummary ss = it.next();
        if (expected.contains(ss.getWindowEnd())) {
          assertFalse(actual.contains(ss.getWindowEnd()));
          actual.add(ss.getWindowEnd());

          assertEquals(3, ss.getTopAppsCount());
          App app1 = ss.getTopApps(0), app2 = ss.getTopApps(1), app3 = ss.getTopApps(2);

          assertEquals("app-b", app1.getBundle());
          assertEquals(600, app1.getAmount());
          assertEquals(2, app1.getCntUsers());

          assertEquals("app-x", app2.getBundle());
          assertEquals(600, app2.getAmount());
          assertEquals(2, app2.getCntUsers());

          assertEquals("app-a", app3.getBundle());
          assertEquals(600, app3.getAmount());
          assertEquals(1, app3.getCntUsers());
        }
      }
      assertEquals(expected, actual);
      return null;
    });

    tp.run();
  }

  @Test public void testSample04() {
    // same as testSample02 but with window size 30 minutes each.
    long ts1 = Instant.parse("2021-05-01T10:15:15.000Z").toEpochMilli(); // 1619864115000
    assertEquals(1619864115000L, ts1);
    long ts2 = Instant.parse("2021-05-01T10:20:15.000Z").toEpochMilli(); // 1619864415000
    assertEquals(1619864415000L, ts2);

    SalesEvent.Builder se = SalesEvent.newBuilder();
    DeviceId id1 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id2 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id3 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id4 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id5 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toUpperCase()).build();
    DeviceId id6 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toLowerCase()).build();

    // all of the following events take place at the same time, once at ts1 and once at ts2.
    // the expected output should contain ONE SalesSummary message
    // (corresponding to the window 10:00:00.000 - 10:29:59.999).
    // The SalesSummary message should contain top 3 apps sorted by amount DESC, cnt_users DESC, bundle ASC.
    // Note - for simplicity, this unit test only checks the contents of the window ending on the minute,
    // but it also checks how many windows (SalesSummary objects) your code outputs.
    List<SalesEvent> inputData = Arrays.asList(//
      se.setId(id1).setAmount(100).setBundle("app-x").build(),//
      se.setId(id2).setAmount(200).setBundle("app-x").build(),//
      se.setId(id3).setAmount(300).setBundle("app-x").build(),//
      se.setId(id4).setAmount(300).setBundle("app-b").build(),//
      se.setId(id5).setAmount(300).setBundle("app-b").build(),//
      se.setId(id6).setAmount(600).setBundle("app-a").build(),//
      se.setId(id1).setAmount(10).setBundle("app-g").build(),//
      se.setId(id2).setAmount(50).setBundle("app-g").build(),//
      se.setId(id3).setAmount(50).setBundle("app-g").build(),//
      se.setId(id4).setAmount(50).setBundle("app-g").build(),//
      se.setId(id5).setAmount(50).setBundle("app-g").build(),//
      se.setId(id6).setAmount(50).setBundle("app-g").build()//
    );

    //    System.out.format("%s\n", String.format("%d %s", ts1,
    //      inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" "))));
    //
    //    System.out.format("%s\n", String.format("%d %s", ts2,
    //      inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" "))));

    PCollection<String> input = tp.apply(Create.of(String.format("%d %s", ts1,
      inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" "))), String
      .format("%d %s", ts2,
        inputData.stream().map(elem -> ProtoUtils.encodeMessageBase64(elem)).collect(Collectors.joining(" ")))));

    PCollection<SalesSummary> output = input.apply(new Parser()).apply(new ComputeSummary(30));

    PAssert.that(output).satisfies(out -> {
      assertEquals(210, Iterables.size(out));
      Set<String> expected = new ImmutableSet.Builder<String>().add("10:29:59").build();
      Set<String> actual = new HashSet<>();

      Iterator<SalesSummary> it = out.iterator();
      while (it.hasNext()) {
        SalesSummary ss = it.next();
        if (expected.contains(ss.getWindowEnd())) {
          assertFalse(actual.contains(ss.getWindowEnd()));
          actual.add(ss.getWindowEnd());

          assertEquals(3, ss.getTopAppsCount());
          App app1 = ss.getTopApps(0), app2 = ss.getTopApps(1), app3 = ss.getTopApps(2);

          assertEquals("app-b", app1.getBundle());
          assertEquals(1200, app1.getAmount());
          assertEquals(2, app1.getCntUsers());

          assertEquals("app-x", app2.getBundle());
          assertEquals(1200, app2.getAmount());
          assertEquals(2, app2.getCntUsers());

          assertEquals("app-a", app3.getBundle());
          assertEquals(1200, app3.getAmount());
          assertEquals(1, app3.getCntUsers());
        }
      }
      assertEquals(expected, actual);
      return null;
    });

    tp.run();
  }

  // --------------------
}
