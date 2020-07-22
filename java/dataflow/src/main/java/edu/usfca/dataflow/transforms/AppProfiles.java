package edu.usfca.dataflow.transforms;

import java.util.Iterator;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

import edu.usfca.dataflow.CorruptedDataException;
import edu.usfca.dataflow.utils.DeviceProfileUtils;
import edu.usfca.protobuf.Bid.Exchange;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.AppProfile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import edu.usfca.protobuf.Profile.DeviceProfile.AppActivity;

public class AppProfiles {
  /**
   * "ComputeAppProfiles" takes in one PCollection of DeviceProfiles.
   *
   * If the input PCollection contains any duplicate Device IDs (recall that uuid is case-insensitive),
   *
   * then it must throw "CorruptedDataException".
   *
   * Otherwise, proceed to produce one AppProfile proto per "bundle" (String, case-sensitive).
   *
   * For each bundle (app), you should aggregate:
   *
   * (1) "bundle": This is unique key (String) for each AppProfile, and is case-sensitive.
   *
   * (2) "user_count": This is the unique number of users (Device IDs) who have this app in their DeviceProfile's
   * AppActivity.
   *
   * (3) "user_count_per_exchange": Same as (2), but it's a map from "Exchange" enum (its integer value) to the number
   * of unique DeviceIDs.
   *
   * (Note that this is simplified when compared to Project 2.)
   *
   * TODO: You can use instructor's reference code from project 2 and modify it (you'll need to fix a couple of things),
   * or reuse yours. Note that either way you'll have to make changes because the requirements / proto definitions have
   * changed slightly (things are simplified).
   */
  public static class ComputeAppProfiles extends PTransform<PCollection<DeviceProfile>, PCollection<AppProfile>> {

    @Override
    public PCollection<AppProfile> expand(PCollection<DeviceProfile> input) {

      //check for null input
      if (input == null) {
        throw new CorruptedDataException("PCollectionList is null or its size is not 2");
      }

      // check for duplicate device IDs.
      PCollection<DeviceProfile> life = input;
      life.apply(ParDo.of(new DeviceProfileUtils.GetDeviceId())).apply(Count.perKey())
          .apply(Filter.by((ProcessFunction<KV<DeviceId, Long>, Boolean>) kv -> {
            if (kv.getValue() > 1L)
              throw new CorruptedDataException("Duplicate DeviceIDs found in Lifetime PC.");
            return false;
          }));
    
      // $bundle, $exchange_key, $value
      final int offset = 1;
      final int arrSize = Exchange.values().length + offset;
      PCollection<AppProfile> results = life.apply(ParDo.of(new EmitData())).apply(Count.perElement())
          .apply(ParDo.of(new DoFn<KV<KV<String, Integer>, Long>, KV<String, KV<Integer, Long>>>() {
            @ProcessElement
            public void process(ProcessContext c) {
              c.output(
                  KV.of(c.element().getKey().getKey(), KV.of(c.element().getKey().getValue(), c.element().getValue())));
            }
          })).apply(Combine.perKey(new CombineFn<KV<Integer, Long>, int[], AppProfile>() {
            @Override
            public int[] createAccumulator() {
              return new int[arrSize];
            }

            @Override
            public int[] addInput(int[] mutableAccumulator, KV<Integer, Long> input) {
              //adjust for bid proto exchange being almost regular
              int key = input.getKey();
              if (key == 21 || key == 22) {
                key -= 10;
              }
              mutableAccumulator[key + offset] = 
                  mutableAccumulator[key + offset] + input.getValue().intValue();
              return mutableAccumulator;
            }

            @Override
            public int[] mergeAccumulators(Iterable<int[]> accumulators) {
              Iterator<int[]> it = accumulators.iterator();
              int[] first = null;
              while (it.hasNext()) {
                int[] next = it.next();
                if (first == null)
                  first = next;
                else {
                  for (int i = 0; i < arrSize; i++) {
                    first[i] += next[i];
                  }
                }
              }
              return first;
            }

            @Override
            public AppProfile extractOutput(int[] accumulator) {
              AppProfile.Builder ap = AppProfile.newBuilder();
              ap.setUserCount(accumulator[LIFE_COUNT + offset]);
              for (Exchange exchange : Exchange.values()) {
                if (exchange == Exchange.UNRECOGNIZED) {
                  continue;
                }
                //accomodate for near-regularity
                int exch = exchange.getNumber();
                if (exch == 21 || exch == 22) {
                  exch -= 10;
                }
                if (accumulator[exch + offset] != 0) {
                  ap.putUserCountPerExchange(exchange.getNumber(), accumulator[exch + offset]);
                }
              }
              return ap.build();
            }
          })).apply(MapElements.into(TypeDescriptor.of(AppProfile.class))
              .via((KV<String, AppProfile> x) -> x.getValue().toBuilder().setBundle(x.getKey()).build()));
      
      return results;
    }
  }

  static final int LIFE_COUNT = -1;

  static class EmitData extends DoFn<DeviceProfile, KV<String, Integer>> {

    @ProcessElement
    public void process(ProcessContext c) {
      DeviceProfile dp = c.element();
      for (AppActivity app : dp.getAppList()) {
          c.output(KV.of(app.getBundle(), LIFE_COUNT));
          for (int exchange : app.getCountPerExchangeMap().keySet()) {
            if (exchange < 0) {
              continue;
            }
            c.output(KV.of(app.getBundle(), exchange));
          }
      }
    }
  }
}

