package edu.usfca.dataflow.transforms;

import com.google.common.collect.Iterables;
import edu.usfca.dataflow.__TestBase;
import edu.usfca.dataflow.utils.GeneralUtils;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Common.SalesEvent;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class __TestParser {
  // This is to help the grading system avoid 'hanging forever'
  // If unit tests keep failing due to 'timeout' on your local machine or when you need to use a debugger,
  // you should comment this out (this is only necessary for the grading system).
  //@Rule public Timeout timeout = Timeout.seconds(10);


  @Rule public final transient TestPipeline tp = TestPipeline.create();

  @Before public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  // -------------------------
  //CigIARIkMzcyMUFGQjUtMjJERS00MDhCLUI5N0QtQ0I2Q0E4OTUzQ0M5EgVhcHAteBhk
  //CigIARIkMzcyMWFmYjUtMjJkZS00MDhiLWI5N2QtY2I2Y2E4OTUzY2M5EgVhcHAteBjIAQ==
  //CigIAhIkMzcyMUFGQjUtMjJERS00MDhCLUI5N0QtQ0I2Q0E4OTUzQ0M5EgVhcHAteBisAg==
  //CigIAhIkMzcyMWFmYjUtMjJkZS00MDhiLWI5N2QtY2I2Y2E4OTUzY2M5EgVhcHAtYhisAg==
  //CiYaJEI1ODZFQUVELTY3ODgtNEU1RS05MDg0LTI2ODE3MDYwNDEwOBIFYXBwLWIYrAI=
  //CiYaJGI1ODZlYWVkLTY3ODgtNGU1ZS05MDg0LTI2ODE3MDYwNDEwOBIFYXBwLWEY2AQ=
  //CigIARIkMzcyMUFGQjUtMjJERS00MDhCLUI5N0QtQ0I2Q0E4OTUzQ0M5EgVhcHAtZxgK
  //CigIARIkMzcyMWFmYjUtMjJkZS00MDhiLWI5N2QtY2I2Y2E4OTUzY2M5EgVhcHAtZxgy
  //CigIAhIkMzcyMUFGQjUtMjJERS00MDhCLUI5N0QtQ0I2Q0E4OTUzQ0M5EgVhcHAtZxgy
  //CigIAhIkMzcyMWFmYjUtMjJkZS00MDhiLWI5N2QtY2I2Y2E4OTUzY2M5EgVhcHAtZxgy
  @Test public void testParser() {
    long ts = Instant.parse("2021-05-01T10:15:15.000Z").getMillis(); // 1619864115000
    System.out.format("%d\n", ts);
    assertEquals(1619864115000L, ts);

    SalesEvent.Builder seb = SalesEvent.newBuilder();
    DeviceId id1 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id2 = DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id3 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toUpperCase()).build();
    DeviceId id4 = DeviceId.newBuilder().setOs(OsType.OS_IOS).setUuid(__TestBase.UUID1.toLowerCase()).build();
    DeviceId id5 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toUpperCase()).build();
    DeviceId id6 = DeviceId.newBuilder().setOs(OsType.OS_UNSPECIFIED).setWebid(__TestBase.UUID2.toLowerCase()).build();

    List<SalesEvent> inputData = Arrays.asList(//
      seb.setId(id1).setAmount(100).setBundle("app-x").build(),//
      seb.setId(id2).setAmount(200).setBundle("app-x").build(),//
      seb.setId(id3).setAmount(300).setBundle("app-x").build(),//
      seb.setId(id4).setAmount(300).setBundle("app-b").build(),//
      seb.setId(id5).setAmount(300).setBundle("app-b").build(),//
      seb.setId(id6).setAmount(600).setBundle("app-a").build(),//
      seb.setId(id1).setAmount(10).setBundle("app-g").build(),//
      seb.setId(id2).setAmount(50).setBundle("app-g").build(),//
      seb.setId(id3).setAmount(50).setBundle("app-g").build(),//
      seb.setId(id4).setAmount(50).setBundle("app-g").build()//
    );

    List<String> inputForParser = new ArrayList<>();
    Map<SalesEvent, Long> expected = new HashMap<>();
    for (int i = 0, index = 0; i < 4; i++) {
      List<String> elem = new ArrayList<>();
      elem.add(String.format("%d", ts + i));
      for (int j = 0; j <= i; j++, index++) {
        elem.add(ProtoUtils.encodeMessageBase64(inputData.get(index)));
        expected.put(GeneralUtils.getNormalized(inputData.get(index)), ts + i);
        System.out.format("%s\n", ProtoUtils.encodeMessageBase64(inputData.get(index)));
      }
      inputForParser.add(elem.stream().collect(Collectors.joining(" ")));
    }
    PCollection<SalesEvent> output = tp.apply(Create.of(inputForParser)).apply(new Parser());

    PAssert.that(output.apply(ParDo.of(new GetElementTimestamp()))).satisfies(out -> {
      assertEquals(10, Iterables.size(out));
      Map<SalesEvent, Long> actual = new HashMap<>();
      for (KV<SalesEvent, Instant> kv : out) {
        actual.put(GeneralUtils.getNormalized(kv.getKey()), kv.getValue().getMillis());
      }
      assertEquals(expected.keySet(), actual.keySet());
      for (SalesEvent se : expected.keySet()) {
        assertEquals(expected.get(se), actual.get(se));
      }
      return null;
    });
    tp.run();
  }

  static class GetElementTimestamp extends DoFn<SalesEvent, KV<SalesEvent, Instant>> {
    @ProcessElement public void process(ProcessContext c) {
      c.output(KV.of(c.element(), c.timestamp()));
    }
  }

  // -----------
}
