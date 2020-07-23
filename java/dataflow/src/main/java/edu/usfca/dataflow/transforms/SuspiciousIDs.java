package edu.usfca.dataflow.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.AppProfile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import edu.usfca.protobuf.Profile.DeviceProfile.AppActivity;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Suspicious IDs looks at various aspects of a given DeviceID to determine whether or not
 * it may be a bot, and thus effecting the efficiency of a given ad campaign.
 * @author Jackson
 * @author Hayden Lee, University of San Francisco
 */
public class SuspiciousIDs {
  private static final Logger LOG = LoggerFactory.getLogger(SuspiciousIDs.class);

  /**
   * This method serves to flag certain users as suspicious.
   *
   * (1) USER_COUNT_THRESHOLD: This determines whether an app is popular or not.
   *
   * Any app that has more than "USER_COUNT_THRESHOLD" unique users is considered popular.
   *
   * Default value is 4 (so, 5 or more users = popular).
   *
   *
   * (2) APP_COUNT_THRESHOLD: If a user (DeviceProfile) contains more than "APP_COUNT_THRESHOLD" unpopular apps,
   *
   * then the user is considered suspicious.
   *
   * Default value is 3 (so, 4 or more unpopular apps = suspicious).
   *
   *
   * (3) GEO_COUNT_THRESHOLD: If a user (DeviceProfile) contains more than "GEO_COUNT_THRESHOLD" unique Geo's,
   *
   * then the user is considered suspicious.
   *
   * Default value is 8 (so, 9 or more distinct Geo's = suspicious).
   *
   * 
   * (4) BID_LOG_COUNT_THRESHOLD: If a user (DeviceProfile) appeared in more than "BID_LOG_COUNT_THRESHOLD" Bid Logs,
   *
   * then the user is considered suspicious (we're not counting invalid BidLogs for this part as it should have been
   * ignored from the beginning).
   *
   * Default value is 10 (so, 11 or more valid BidLogs from the same user = suspicious).
   *
   * The default values are mainly for unit tests (so you can easily check correctness with rather small threshold
   * values).
   */

  public static PCollection<DeviceId> getSuspiciousIDs(//
      PCollection<DeviceProfile> dps, //
      PCollection<AppProfile> aps, //
      int USER_COUNT_THRESHOLD, // Default is 4 for unit tests.
      int APP_COUNT_THRESHOLD, // Default is 3 for unit tests.
      int GEO_COUNT_THRESHOLD, // Default is 8 for unit tests.
      int BID_LOG_COUNT_THRESHOLD // Default is 10 for unit tests.
  ) {
    LOG.info("[Thresholds] user count {} app count {} geo count {} bid log count {}", USER_COUNT_THRESHOLD,
        APP_COUNT_THRESHOLD, GEO_COUNT_THRESHOLD, BID_LOG_COUNT_THRESHOLD);
    
    PCollectionView<List<String>> unpopularAppsView = aps
        .apply(Filter.by((ProcessFunction<AppProfile, Boolean>) app -> app.getUserCount() <= USER_COUNT_THRESHOLD))
        .apply(ParDo.of(new DoFn<AppProfile, String>() {
          @ProcessElement
          public void process(ProcessContext c) {
            c.output(c.element().getBundle());
          }
        }))
        .apply(View.asList());
    PCollection<DeviceProfile> suspiciousDps = dps
        .apply("suspiciousDoFn", ParDo.of(new DoFn<DeviceProfile, DeviceProfile>() {
          
          Set<String> sapps;
          List<DeviceProfile> buffer;
          int BUFFER_MAX;
          
          @StartBundle
          public void startBundle() {
            buffer = new ArrayList<>();
            sapps  = new HashSet<>();
            BUFFER_MAX = 20;
          }

          @ProcessElement
          public void process(ProcessContext c) {
            int appCount = 0;
            boolean output = false;
            if (sapps.isEmpty()) {
              sapps.addAll(c.sideInput(unpopularAppsView));
            }
            int bidCount = 0;
            
            if (c.element().getGeoCount() > GEO_COUNT_THRESHOLD) {
              buffer.add(c.element());
              if (buffer.size() >= BUFFER_MAX) {
                flush(c);
              }
              output = true;
            }
            
            if (!output) {
              for (AppActivity aa : c.element().getAppList()) {
                String bundle = aa.getBundle();
                if (sapps.contains(bundle)) {
                  appCount++;
                  if (appCount > APP_COUNT_THRESHOLD) {
                    buffer.add(c.element());
                    if (buffer.size() >= BUFFER_MAX) {
                      flush(c);
                    }
                    output = true;
                    break;
                  }
                }
                for (Integer key : aa.getCountPerExchangeMap().keySet()) {
                  bidCount += aa.getCountPerExchangeMap().get(key);
                  if (bidCount > BID_LOG_COUNT_THRESHOLD) {
                    buffer.add(c.element());
                    if (buffer.size() >= BUFFER_MAX) {
                      flush(c);
                    }                
                    output = true;
                    break;
                  }
                }
                if (output) {
                  break;
                }
              }
            }
          }
          
          @FinishBundle
          public void finishBundle(FinishBundleContext c) {
            if (!buffer.isEmpty()) {
              for (DeviceProfile dp : buffer) {
                c.output(dp, Instant.EPOCH, GlobalWindow.INSTANCE);
              }
            }
          }
          
          public void flush(ProcessContext c) {
            for (DeviceProfile dp : buffer) {
              c.output(dp);
            }
            buffer.clear();
          } 
        }).withSideInputs(unpopularAppsView));
    
    PCollection<DeviceId> ids = suspiciousDps.apply(ParDo.of(new DoFn<DeviceProfile, DeviceId>() {
      @ProcessElement
      public void process(ProcessContext c) {
        c.output(c.element().getDeviceId());
      }
    }));

    return ids;
  }
}
