package edu.usfca.dataflow.transforms;

import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Tensor;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Data.PredictionData;

public class Predictions {

  /**
   * This method will be called by the unit tests.
   *
   * The reason for having this method (instead of instantiating a specific DoFn) is to allow you to easily experiment
   * with different implementations of PredictDoFn.
   *
   * The provided code (see below) for "PredictDoFnNever" is "correct" but extremely inefficient.
   *
   * Use it as a reference to implement "PredictDoFn" instead.
   *
   * When you are ready to optimize it, you'll find the ungraded homework for Lab 09 useful (as well as sample code from
   * L34: DF-TF).
   */
  public static DoFn<KV<DeviceId, float[]>, PredictionData> getPredictDoFn(String pathToModelDir) {
    return new PredictDoFn(pathToModelDir);
  }

  // This utility method simply returns the index with largest prediction score.
  // Input must be an array of length 10.
  // This is provided for you (see PredictDoFnNever" to understand how it's used).
  static int getArgMax(float[] pred) {
    int prediction = -1;
    for (int j = 0; j < 10; j++) {
      if (prediction == -1 || pred[prediction] < pred[j]) {
        prediction = j;
      }
    }
    return prediction;
  }

  /**
   * Given (DeviceId, float[]) pairs, this DoFn will return (for each element) its prediction & score.
   *
   * Prediction must be between 0 and 9 (inclusive), which "classifies" the input.
   *
   * Score is a numerical value that quantifies the model's confidence.
   *
   * The model returns 10 values (10 scores), and you should use the provided "getArgMax" method to obtain its
   * classification and score.
   *
   * As final output, PredictionData (proto) should be returned, which has two fields (DeviceId and double).
   *
   * NOTE: It's strongly recommended that you not change this code (so you can "keep" it as reference),
   *
   * and instead start implementing your own in PredictDoFn below. Then, once you're ready, simply change
   * "getPredictDoFn" above to return an instance of your new DoFn.
   */
  static class PredictDoFnNever extends DoFn<KV<DeviceId, float[]>, PredictionData> {
    final static String tfTag = "serve"; // <- Do not change this.
    final String pathToModelDir;

    transient Tensor rate;
    transient SavedModelBundle mlBundle;

    public PredictDoFnNever(String pathToModelDir) {
      this.pathToModelDir = pathToModelDir;
    }

    // This method is provided for your convenience. Use it as a reference.
    // "inputFeatures" (float[][]) is assumed to be of size "1" by "768".
    // Note: Tensor<> objects are resources that must be explicitly closed to prevent memory leaks.
    // That's why you are seeing the try blocks below (with that, those resources are auto-closed).
    float[][] getPrediction(float[][] inputFeatures, SavedModelBundle mlBundle) {
      // "prediction" array will store the scores returned by the model (recall that the model returns 10 scores per
      // input).
      float[][] prediction = new float[1][10];
      try (Tensor<?> x = Tensor.create(inputFeatures)) {
        try (Tensor<?> output = mlBundle.session().runner().feed("input_tensor", x).feed("dropout/keep_prob", rate)
            .fetch("output_tensor").run().get(0)) {
          output.copyTo(prediction);
        }
      }
      return prediction;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      // --------------------------------------------------------------------------------
      // Loading the model:
      mlBundle = SavedModelBundle.load(pathToModelDir, tfTag);

      // This is necessary because the model expects to be given this additional tensor.
      // (For those who're familiar with NNs, keep_prob is 1 - dropout.)
      float[] keep_prob_arr = new float[1024];
      Arrays.fill(keep_prob_arr, 1f);
      rate = Tensor.create(new long[] {1, 1024}, FloatBuffer.wrap(keep_prob_arr));

      // --------------------------------------------------------------------------------

      // Prepare the input data (to be fed to the model).
      final DeviceId id = c.element().getKey();
      final float[][] inputData = new float[][] {c.element().getValue()};

      // Obtain the prediction scores from the model, and Find the index with maximum score (ties broken by favoring
      // smaller index).
      float[][] pred = getPrediction(inputData, mlBundle);
      int prediction = getArgMax(pred[0]);

      // Build PredictionData proto and output it.
      PredictionData.Builder pd =
          PredictionData.newBuilder().setId(id).setPrediction(prediction).setScore(pred[0][prediction]);
      c.output(pd.build());
    }
  }

  /**
   * TODO: Use this starter code to implement your own PredictDoFn.
   *
   * You'll need to utilize DoFn's annotated methods & optimization techniques that we discussed in L10, L30, L34, and
   * Lab09.
   */
  static class PredictDoFn extends DoFn<KV<DeviceId, float[]>, PredictionData> {
    final static String tfTag = "serve"; // <- Do not change this.
    final String pathToModelDir;
    
    List<PredictionData> buffer;
    int BUFFER_MAX;

    transient Tensor rate;
    transient SavedModelBundle mlBundle;

    public PredictDoFn(String pathToModelDir) {
      this.pathToModelDir = pathToModelDir;
    }
    
    // This method is provided for your convenience. Use it as a reference.
    // "inputFeatures" (float[][]) is assumed to be of size "1" by "768".
    // Note: Tensor<> objects are resources that must be explicitly closed to prevent memory leaks.
    // That's why you are seeing the try blocks below (with that, those resources are auto-closed).
    float[][] getPrediction(float[][] inputFeatures, SavedModelBundle mlBundle) {
      // "prediction" array will store the scores returned by the model (recall that the model returns 10 scores per
      // input).
      float[][] prediction = new float[1][10];
      try (Tensor<?> x = Tensor.create(inputFeatures)) {
        try (Tensor<?> output = mlBundle.session().runner().feed("input_tensor", x).feed("dropout/keep_prob", rate)
            .fetch("output_tensor").run().get(0)) {
          output.copyTo(prediction);
        }
      }
      return prediction;
    }

    @Setup
    public void setup() {
      // Loading the model:
      mlBundle = SavedModelBundle.load(pathToModelDir, tfTag);

      // This is necessary because the model expects to be given this additional tensor.
      // (For those who're familiar with NNs, keep_prob is 1 - dropout.)
      float[] keep_prob_arr = new float[1024];
      Arrays.fill(keep_prob_arr, 1f);
      rate = Tensor.create(new long[] {1, 1024}, FloatBuffer.wrap(keep_prob_arr));
    }
    
    @StartBundle
    public void startBundle() {
      buffer = new ArrayList<>();
      BUFFER_MAX = 50;
    }
    
    @ProcessElement
    public void process(ProcessContext c) {
      // Prepare the input data (to be fed to the model).
      final DeviceId id = c.element().getKey();
      final float[][] inputData = new float[][] {c.element().getValue()};

      // Obtain the prediction scores from the model, and Find the index with maximum score (ties broken by favoring
      // smaller index).
      float[][] pred = getPrediction(inputData, mlBundle);
      int prediction = getArgMax(pred[0]);

      // Build PredictionData proto and output it.
      PredictionData.Builder pd =
          PredictionData.newBuilder().setId(id).setPrediction(prediction).setScore(pred[0][prediction]);
      buffer.add(pd.build());
      if (buffer.size() >= BUFFER_MAX) {
        flush(c);
      }
    }
    
    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      if (!buffer.isEmpty()) {
        for (PredictionData pd : buffer) {
          c.output(pd, Instant.EPOCH, GlobalWindow.INSTANCE);
        }
      }
    }
    
    public void flush(ProcessContext c) {
      for (PredictionData pd : buffer) {
        c.output(pd);
      }
      buffer.clear();
    }
    
  }
}
