package edu.usfca.dataflow.transforms;

import java.util.Map;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.google.protobuf.InvalidProtocolBufferException;

import edu.usfca.dataflow.CorruptedDataException;
import edu.usfca.dataflow.utils.DeviceProfileUtils;
import edu.usfca.dataflow.utils.PredictionUtils;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.DeviceProfile; 
import edu.usfca.protobuf.Profile.InAppPurchaseProfile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class Features {
  /**
   * This PTransform takes a PCollectionList that contains three PCollections of Strings.
   *
   * 1. DeviceProfile (output from the first pipeline) with unique DeviceIDs,
   *
   * 2. DeviceId (output from the first pipeline) that are "suspicious" (call this SuspiciousIDs), and
   *
   * 3. InAppPurchaseProfile (separately provided) with unique bundles.
   *
   * All of these proto messages are Base64-encoded (you can check ProtoUtils class for how to decode that, e.g.).
   *
   * [Step 1] First, in this PTransform, you must filter out (remove) DeviceProfiles whose DeviceIDs are found in the
   * SuspiciousIDs as we are not going to consider suspicious users.
   *
   * [Step 2] Next, you ALSO filter out (remove) DeviceProfiles whose DeviceID's UUIDs are NOT in the following form:
   *
   * ???????0-????-????-????-????????????
   *
   * Effectively, this would "sample" the data at rate (1/16). This sampling is mainly for efficiency reasons (later
   * when you run your pipeline on GCP, the input data is quite large as you will need to make "predictions" for
   * millions of DeviceIDs).
   *
   * To be clear, if " ...getUuid().charAt(7) == '0' " is true, then you process the DeviceProfile; otherwise, ignore
   * it.
   *
   * [Step 3] Then, for each user (DeviceProfile), use the method in
   * {@link edu.usfca.dataflow.utils.PredictionUtils#getInputFeatures(DeviceProfile, Map)} to obtain the user's
   * "Features" (to be used for TensorFlow model). See the comments for this method.
   *
   * Note that the said method takes in a Map (in addition to DeviceProfile) from bundles to IAPP, and thus you will
   * need to figure out how to turn PCollection into a Map. We have done this in the past (in labs & lectures).
   *
   */
  public static class GetInputToModel extends PTransform<PCollectionList<String>, PCollection<KV<DeviceId, float[]>>> {

    @Override
    public PCollection<KV<DeviceId, float[]>> expand(PCollectionList<String> pcList) {
      // TODO: If duplicate deviceIDs are found, throw CorruptedDataException.
      // Note that UUIDs are case-insensitive.

      //DeviceIds
      PCollectionView<List<DeviceId>> idView =
          pcList.get(1).apply(MapElements.into(TypeDescriptor.of(DeviceId.class)).via((String b64) -> {
            try {
              return ProtoUtils.decodeMessageBase64(DeviceId.parser(), b64);
            } catch (InvalidProtocolBufferException e) {
              e.printStackTrace();
            }
            return null;
          })).apply(View.asList());
      
      //InAppPurchaserProfiles
      PCollection<InAppPurchaseProfile> iapp =
          pcList.get(2).apply(MapElements.into(TypeDescriptor.of(InAppPurchaseProfile.class)).via((String b64) -> {
            try {
              return ProtoUtils.decodeMessageBase64(InAppPurchaseProfile.parser(), b64);
            } catch (InvalidProtocolBufferException e) {
              e.printStackTrace();
            }
            return null;
          }));
      
      //DeviceProfiles
      PCollection<DeviceProfile> dp =
          pcList.get(0).apply(MapElements.into(TypeDescriptor.of(DeviceProfile.class)).via((String b64) -> {
            try {
              return ProtoUtils.decodeMessageBase64(DeviceProfile.parser(), b64);
            } catch (InvalidProtocolBufferException e) {
              e.printStackTrace();
            }
            return null;
          }));
      
      //Filter DeviceProfiles with non-unique deviceIds
        dp.apply(ParDo.of(new DeviceProfileUtils.GetDeviceId())).apply(Count.perKey())
          .apply(Filter.by((ProcessFunction<KV<DeviceId, Long>, Boolean>) kv -> {
            if (kv.getValue() > 1L)
              throw new CorruptedDataException("Duplicate DeviceIDs found in Lifetime PC.");
            return false;
          }));
      
      //Filtered profiles
      PCollection<DeviceProfile> filtered = dp.apply(ParDo.of(new DoFn<DeviceProfile, DeviceProfile>() {
          
        List<DeviceId> ids;
        @ProcessElement
        public void process(ProcessContext c) {
          if (ids == null) {
            ids = new ArrayList<>();
            ids.addAll(c.sideInput(idView));
          }
          DeviceId id = c.element().getDeviceId();
          if (!ids.contains(id) && id.getUuid().charAt(7) == '0') {
            c.output(c.element());
          }
      }}).withSideInputs(idView));
      
      //Make mapView
      PCollectionView<Map<String, InAppPurchaseProfile>> iappMapView = iapp
          .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), 
              TypeDescriptor.of(InAppPurchaseProfile.class)))
              .via((InAppPurchaseProfile x) -> KV.of(x.getBundle(), x)))
          .apply(View.asMap());
      
      //Get results
      PCollection<KV<DeviceId, float[]>> results = filtered
          .apply(ParDo.of(new DoFn<DeviceProfile, KV<DeviceId, float[]>>() {
            
            Map<String, InAppPurchaseProfile> iappMap;
            
            @ProcessElement
            public void process(ProcessContext c) {
              if (iappMap == null) {
                iappMap = new HashMap<>();
                iappMap.putAll(c.sideInput(iappMapView));
              }
              c.output(KV.of(c.element().getDeviceId(), PredictionUtils.getInputFeatures(c.element(), iappMap)));
            }}).withSideInputs(iappMapView));
   
      return results;
    }
  }
}
