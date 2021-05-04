package edu.usfca.dataflow.utils;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.SalesEvent;
import edu.usfca.protobuf.Common.SalesSummary.App;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Comparator;

public class GeneralUtils {

  // Consider this method correct, and use it if necessary.
  // Reference solution uses this.
  public static String formatWindowEnd(long windowEnd) {
    long hourMinuteSecond = (windowEnd / 1000L) % (24L * 60L * 60L);
    return String
      .format("%02d:%02d:%02d", hourMinuteSecond / 3600L, (hourMinuteSecond / 60L) % 60L, hourMinuteSecond % 60L);
  }

  /**
   * Using this, we can sort apps by their sales amount (in descending order);
   * ties are broken by cnt_users (in descending order);
   * further ties are broken by bundle (in ascending order, using String's compareTo()).
   * <p>
   * see __TestGeneralUtils.testAppComparator.
   */
  // The default behavior of this Comparator is to sort Apps in descending order of amount.
  // You can optionally specify a parameter to sort them in ascending order.
  public static class AppComparator implements Comparator<App>, Serializable {
    final boolean descending;

    public AppComparator() {
      descending = true;
    }

    public AppComparator(boolean descending) {
      this.descending = descending;
    }

    @Override public int compare(App a, App b) {
      // TODO: implement this. You will want to use "descending" flag to determine what to return here.
      return 0;
    }
  }

  // You may assume that this method is correct. It's used by reference solution and unit tests.
  public static SalesEvent getNormalized(SalesEvent msg) {
    DeviceId id = msg.getId();
    if (!StringUtils.isBlank(id.getUuid())) {
      return msg.toBuilder().setId(id.toBuilder().setUuid(id.getUuid().toLowerCase())).build();
    }
    return msg;
  }
}
