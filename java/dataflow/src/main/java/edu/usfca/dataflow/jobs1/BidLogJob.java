package edu.usfca.dataflow.jobs1;

import static edu.usfca.dataflow.transforms.SuspiciousIDs.getSuspiciousIDs;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.usfca.dataflow.MyOptions;
import edu.usfca.dataflow.transforms.AppProfiles.ComputeAppProfiles;
import edu.usfca.dataflow.utils.PathConfigs;
import edu.usfca.dataflow.utils.BidLogUtils;
import edu.usfca.dataflow.utils.DeviceProfileUtils;
import edu.usfca.dataflow.utils.IOUtils;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Bid.BidLog;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.AppProfile;
import edu.usfca.protobuf.Profile.DeviceProfile;

import com.google.protobuf.InvalidProtocolBufferException; 

/**
 * BidLogJob reads TFRecords to generate DeviceProfiles and merge them into a PCollection<DeviceProfile>.
 * Next, ApplicationProfiles are generated (data about some particular application) from the above PCollection,
 * and a PCollection of SuspiciousIDs (potentially bot accounts) are generated from these PCollection<DeviceProfile> 
 * and PCollection<Applicationprofile>.
 * @author Jackson Raffety
 * @author Hayden Lee, University of San Francisco
 */
public class BidLogJob {
  private static final Logger LOG = LoggerFactory.getLogger(BidLogJob.class);

  /**
   *
   * If all unit tests pass, run your job locally (using DirectRunner) by using the following command:
   *
   * Under "java/dataflow" directory (of your local repo):
   *
   * gradle run -Pargs="--job=bidLogJob --bidLogCountThreshold=115 --geoCountThreshold=1 --userCountThreshold=2
   * --appCountThreshold=18 --pathToResourceRoot=/Users/haden/usf/resources/project5-actual"
   *
   * (Note that the last flag can be omitted if that's the path you already set in Main method.)
   *
   * When this job runs successfully (either on your machine or on GCP), it should output three files, one file under
   * each subdirectory of the "output" directory of your LOCAL_PATH_TO_RESOURCE_DIR (if you ran it locally).
   *
   * DeviceProfile: Merged DeviceProfile data (per user=DeviceId). We'll treat this dataset as "lifetime"
   * DeviceProfile dataset. Revisit Project 2 for the details (or see DeviceProfileUtils class). You should see 707
   * lines in the file.
   *
   * AppProfile: AppProfile data (generated using DeviceProfile data from above). To simplify things, we are only
   * counting lifetime user count and also count per exchange. You should see 510 lines in the file if using 
   * provided dataset.
   *
   * Suspicious (Device): If a certain device has *a lot of apps* that few people use, the said device may be a bot
   * (not a normal human user). In this project, a DeviceId is considered "suspicious" if it has enough unpopular apps
   * (where an app is unpopular if the unique number of users is small). In addition, if a device "appeared" in too many
   * geological locations, that's also considered suspicious. You should see 6 lines in the file if using provided dataset.
   *
   * For all three datasets mentioned above, we write text files in Base64 encoding. Some of these will be used
   * in the second job (pipeline), called PredictionJob.
   *
   * Note that "sample output" files are provided (in the "output-reference" directory), as they should be used as input
   * to the second pipeline, but the contents of the sample output could be different from the contents of your output
   * (e.g., the order of the lines can be different as PCollections do not preserve the order of elements).
   */

  public static class BidLog2DeviceProfile extends PTransform<PCollection<byte[]>, PCollection<DeviceProfile>> {

    @Override
    public PCollection<DeviceProfile> expand(PCollection<byte[]> bidLogBinary) {
      // Note that the input PCollection contains BidLog protos (but serializeD).
      PCollection<BidLog> logs = bidLogBinary.apply(ParDo.of(new DoFn<byte[], BidLog>() {
        @ProcessElement
        public void process(ProcessContext c) throws InvalidProtocolBufferException {
          c.output(BidLog.parseFrom(c.element()));
        }
      }));
      return logs.apply(Filter.by((ProcessFunction<BidLog, Boolean>) bidLog -> BidLogUtils.isValid(bidLog)))
          .apply(ParDo.of(new DoFn<BidLog, KV<DeviceId, DeviceProfile>>() {
            @ProcessElement
            public void process(ProcessContext c) {
              DeviceProfile dp = BidLogUtils.getDeviceProfile(c.element());
              c.output(KV.of(dp.getDeviceId(), dp));
            }
          }))
          .apply(Combine.perKey(new SerializableFunction<Iterable<DeviceProfile>, DeviceProfile>() {
            @Override
            public DeviceProfile apply(Iterable<DeviceProfile> dps) {
              return DeviceProfileUtils.mergeDps(dps);
            }
          })).apply(Values.create());
    }
  }

  public static void execute(MyOptions options) {
    LOG.info("Options: {}", options.toString());
    // ----------------------------------------------------------------------
    // You should NOT change what's in PathConfigs class, but DO take a look to understand how input/output paths
    // are decided in this project. Likewise, DO take a look at "MyOptions" class.
    final PathConfigs config = PathConfigs.of(options);
    Pipeline p = Pipeline.create(options);

    // 1. Read BidLog data from TFRecord files, create DeviceProfiles, and return merged DeviceProfiles.
    PCollection<byte[]> rawData = p.apply(TFRecordIO.read().from(config.getReadPathToBidLog()));
    PCollection<DeviceProfile> deviceProfiles = rawData.apply(new BidLog2DeviceProfile());

    // 2. Obtain AppProfiles.
    PCollection<AppProfile> appProfiles = deviceProfiles.apply(new ComputeAppProfiles());

    // 3. Suspicious users (IDs).
    PCollection<DeviceId> suspiciousUsers = getSuspiciousIDs(deviceProfiles, appProfiles, //
        options.getUserCountThreshold(), options.getAppCountThreshold(), options.getGeoCountThreshold(),
        options.getBidLogCountThreshold());
    
    // 4. Output (write to GCS).
    // For convenience, we'll use Base64 encoding.
     IOUtils.encodeB64AndWrite(deviceProfiles, config.getWritePathToDeviceProfile());
     IOUtils.encodeB64AndWrite(appProfiles, config.getWritePathToAppProfile());
     IOUtils.encodeB64AndWrite(suspiciousUsers, config.getWritePathToSuspiciousUser());

    p.run().waitUntilFinish();
  }
}
