package edu.dartmouth.hmmem;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViterbiMapReduce {

	public static class ViterbiMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, TaggedObservationSequence> {

		private static final Logger logger = LoggerFactory.getLogger(ViterbiMapReduce.class);

		public static final String BUCKET_URI_KEY = "bucket_uri";
		public static final String MODEL_PARAMETERS_DIR_PATH_KEY = "model_parameters_file_path";
		public static final String START_STATE_KEY = "start_state";

		private final Map<StringPair, Double> transLogProbMap = new HashMap<>();
		private final Map<StringPair, Double> emisLogProbMap = new HashMap<>();

		private Set<String> stateSet;
		private String startState;

		private boolean failure = false;
		private String failureString;

		@Override
		public void map(LongWritable byteOffset, Text value,
				OutputCollector<NullWritable, TaggedObservationSequence> output, Reporter reporter) throws IOException {
			if (failure) {
				throw new IOException(failureString);
			}

			System.out.println("Transitions:");
			for (Entry<StringPair, Double> entry : transLogProbMap.entrySet()) {
				System.out.println(entry.getKey() + ": " + entry.getValue());
			}

			System.out.println("Emissions:");
			for (Entry<StringPair, Double> entry : emisLogProbMap.entrySet()) {
				System.out.println(entry.getKey() + ": " + entry.getValue());
			}

			System.out.println("State set:");
			for (String state : stateSet) {
				System.out.println(state);
			}

			System.out.println("Start state:");
			System.out.println(startState);

			// 跳过空值，这里主要是空行
			if (value.toString().replaceAll("\\s+", "").length() == 0) {
				return;
			}

			// 从输入文件中创建观察序列列.
			String observationSequenceString = value.toString();
			List<String> observationSequence = Arrays.asList(observationSequenceString.trim().split("\\s+"));

			// 计算Viterbi标注
			TaggedObservationSequence viterbiTagging = calculateViterbiTagging(observationSequence, transLogProbMap,
					emisLogProbMap, stateSet, startState);
			viterbiTagging.setByteOffset(byteOffset);

			// 输出已经标注的序列
			output.collect(NullWritable.get(), viterbiTagging);
		}

		/**
		 * Runs before each map. Obtains the path to the model parameters file from the job conf. Then
		 * parses the file and fills in the transition and emission log probabilities maps.
		 */
		@Override
		public void configure(JobConf job) {
			try {
				logger.info("Configure");

				System.err.println("~~~~~~~~~~~~~Configure~~~~~~~~~~~~~");

				URI bucketURI = new URI(job.get(BUCKET_URI_KEY));

				FileSystem fs;
				fs = NativeS3FileSystem.get(bucketURI, new Configuration());

				Path modelParametersDirPath = new Path(job.get(MODEL_PARAMETERS_DIR_PATH_KEY));
				FileStatus[] modelParameterFileStatuses;
				modelParameterFileStatuses = fs.listStatus(modelParametersDirPath);

				for (FileStatus modelParameterFileStatus : modelParameterFileStatuses) {
					logger.info("Parsing model parameters file: " + modelParameterFileStatus.getPath());

					if (!modelParameterFileStatus.getPath().getName()
							.equals(MaximizationReducer.TOTAL_LOG_ALPHA_FILE_NAME)) {
						FSDataInputStream modelParametersIn = fs.open(modelParameterFileStatus.getPath());
						StaticUtil.readModelParametersFile(modelParametersIn, transLogProbMap, emisLogProbMap);
					}
				}

				logger.info("End of configure()");
			} catch (Exception e) {
				failure = true;
				failureString = e.toString();

				logger.error(failureString);
			}

			// 设置stateSet和startState
			startState = job.get(START_STATE_KEY);
			stateSet = StaticUtil.makeStateSetFromTransDict(transLogProbMap);
		}

		private static TaggedObservationSequence calculateViterbiTagging(List<String> observationSequence,
				Map<StringPair, Double> transLogProbDict, Map<StringPair, Double> emisLogProbDict,
				Set<String> stateSet, String startState) {
			int numObs = observationSequence.size();

			// Keeps track of probabilities.
			Map<String, Double[]> viterbiLogProbMatrix = new HashMap<String, Double[]>();
			// Keeps track of previous tag that maximized probability.
			Map<String, String[]> viterbiPrevStateMatrix = new HashMap<String, String[]>();

			for (String state : stateSet) {
				viterbiLogProbMatrix.put(state, new Double[numObs]);
				viterbiPrevStateMatrix.put(state, new String[numObs]);
			}

			// Begin by filling out the matrix for the first observation. This requires a
			// special case because we start out at startState with a probability of 1.0.
			for (String state : stateSet) {
				// P(state|#) * P(firstObservation|state)
				StringPair transStringPair = new StringPair(startState, state);
				Double logProbStateGivenStart = transLogProbDict.get(transStringPair);

				StringPair emisStringPair = new StringPair(state, observationSequence.get(0));
				Double logProbObsGivenState = emisLogProbDict.get(emisStringPair);

				Double logProb = StaticUtil.calcLogProductOfLogs(logProbStateGivenStart, logProbObsGivenState);
				viterbiLogProbMatrix.get(state)[0] = logProb;
			}

			// Now complete the rest of the matrix.
			for (int i = 1; i < numObs; i++) {
				String obs = observationSequence.get(i);

				for (String state : stateSet) {
					// The emission probability is the same given one state.
					StringPair emisStringPair = new StringPair(state, obs);
					Double logProbObsGivenState = emisLogProbDict.get(emisStringPair);

					for (String prevState : stateSet) {
						// P(state|prevState) * P(obs|state) * Forward(i-1, prevState)
						StringPair transStringPair = new StringPair(prevState, state);
						Double logProbStateGivenPrevState = transLogProbDict.get(transStringPair);

						Double prevObsPrevStateViterbiLogProb = viterbiLogProbMatrix.get(prevState)[i - 1];

						Double logProb = StaticUtil.calcLogProductOfLogs(
								StaticUtil.calcLogProductOfLogs(logProbStateGivenPrevState, logProbObsGivenState),
								prevObsPrevStateViterbiLogProb);
						Double maxLogProb = viterbiLogProbMatrix.get(state)[i];
						if (maxLogProb == null || (logProb != null && logProb > maxLogProb)) {
							viterbiLogProbMatrix.get(state)[i] = logProb;
							viterbiPrevStateMatrix.get(state)[i] = prevState;
						}
					}
				}
			}

			// Find the tagging for the last emission in the sequence.
			Double lastMaxLogProb = null;
			String lastMaxState = null;
			for (String state : stateSet) {
				Double lastLogProb = viterbiLogProbMatrix.get(state)[numObs - 1];
				if (lastMaxLogProb == null || (lastLogProb != null && lastLogProb > lastMaxLogProb)) {
					lastMaxLogProb = lastLogProb;
					lastMaxState = state;
				}
			}

			// Create the optimal tagging from the matrices.
			TaggedObservationSequence optimalTagging = new TaggedObservationSequence();

			String state = lastMaxState;
			for (int i = numObs - 1; i >= 0; i--) {
				String obs = observationSequence.get(i);
				optimalTagging.prependObsTag(new StringPair(obs, state));

				// Update state for the preceding observation.
				state = viterbiPrevStateMatrix.get(state)[i];
			}

			// Print matrices
			System.out.println("Viterbi Log Prob Matrix:");
			for (Entry<String, Double[]> entry : viterbiLogProbMatrix.entrySet()) {
				System.out.print(entry.getKey() + ":");
				for (Double logProb : entry.getValue()) {
					System.out.print(" " + logProb);
				}

				System.out.println();
			}

			System.out.println("Viterbi Prev State Matrix:");
			// Print matrices
			for (Entry<String, String[]> entry : viterbiPrevStateMatrix.entrySet()) {
				System.out.print(entry.getKey() + ":");
				for (String prevState : entry.getValue()) {
					System.out.print(" " + prevState);
				}

				System.out.println();
			}

			return optimalTagging;
		}
	}

}
