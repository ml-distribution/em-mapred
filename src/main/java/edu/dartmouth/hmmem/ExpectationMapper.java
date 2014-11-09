package edu.dartmouth.hmmem;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ExpectationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, EMModelParameter> {

	private static final Logger LOGGER = Logger.getLogger(ExpectationMapper.class.toString());

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
	public void map(LongWritable key, Text value, OutputCollector<Text, EMModelParameter> output, Reporter reporter)
			throws IOException {
		LOGGER.log(Level.INFO, "ExpectationMapper");

		System.err.println("~~~~~~~~~~~~~ExpectationMapper~~~~~~~~~~~~~");

		if (failure) {
			throw new IOException(failureString);
		}

		// Create the observation sequence list from the input line.
		String observationSequenceString = value.toString();
		String[] observationSequenceArray = observationSequenceString.trim().split("\\s+");

		List<String> observationSequence = new ArrayList<String>();
		for (String obs : observationSequenceArray) {
			String trimmedObs = obs.trim();
			if (trimmedObs.length() != 0) {
				observationSequence.add(trimmedObs);
			}
		}

		if (observationSequence.size() == 0) {
			return;
		}

		// Calculate the forward and backward matrices for the observation sequence.
		Map<String, Double[]> forwardMatrix = calculateForwardMatrix(observationSequence, transLogProbMap,
				emisLogProbMap, stateSet, startState);
		Map<String, Double[]> backwardMatrix = calculateBackwardMatrix(observationSequence, transLogProbMap,
				emisLogProbMap, stateSet, startState);

		// Output the total alpha for the observation sequence under the given model.
		Double logAlpha = getLogAlpha(forwardMatrix);
		if (logAlpha != null) {
			EMModelParameter alpha = EMModelParameter.makeAlphaObject(logAlpha);
			System.out.println("Log alpha: " + logAlpha);
			System.out.println("Alpha object: " + alpha);
			output.collect(EMModelParameter.ALPHA_DUMMY_TEXT, alpha);
		}

		// Calculate the transition and emission counts for the observation sequence under the given model.
		Map<StringPair, Double> transLogCounts = calculateLogTransitionCounts(forwardMatrix, backwardMatrix, logAlpha,
				transLogProbMap, emisLogProbMap, observationSequence, stateSet, startState);
		Map<StringPair, Double> emisLogCounts = calculateLogEmissionCounts(forwardMatrix, backwardMatrix, logAlpha,
				observationSequence, stateSet);

		// Output the transition and emission counts.
		outputTransitionLogCounts(transLogCounts, output);
		outputEmissionLogCounts(emisLogCounts, output);
	}

	/**
	 * Runs before each map. Obtains the path to the model parameters file from the job conf. Then
	 * parses the file and fills in the transition and emission log probabilities maps.
	 */
	@Override
	public void configure(JobConf job) {
		try {
			LOGGER.log(Level.INFO, "Configure");

			System.err.println("~~~~~~~~~~~~~Configure~~~~~~~~~~~~~");

			URI bucketURI = new URI(job.get(BUCKET_URI_KEY));

			FileSystem fs;
			fs = NativeS3FileSystem.get(bucketURI, new Configuration());

			Path modelParametersDirPath = new Path(job.get(MODEL_PARAMETERS_DIR_PATH_KEY));
			FileStatus[] modelParameterFileStatuses;
			modelParameterFileStatuses = fs.listStatus(modelParametersDirPath);

			for (FileStatus modelParameterFileStatus : modelParameterFileStatuses) {
				LOGGER.log(Level.INFO, "Parsing model parameters file: " + modelParameterFileStatus.getPath());

				if (!modelParameterFileStatus.getPath().getName().equals(MaximizationReducer.TOTAL_LOG_ALPHA_FILE_NAME)) {
					FSDataInputStream modelParametersIn = fs.open(modelParameterFileStatus.getPath());
					StaticUtil.readModelParametersFile(modelParametersIn, transLogProbMap, emisLogProbMap);
				}
			}

			LOGGER.log(Level.INFO, "End of configure()");
		} catch (Exception e) {
			failure = true;
			failureString = e.toString();

			LOGGER.log(Level.SEVERE, failureString);
		}

		// Set stateSet and startState.
		startState = job.get(START_STATE_KEY);
		stateSet = StaticUtil.makeStateSetFromTransDict(transLogProbMap);
	}

	/**
	 * Calculates the forward matrix given an observation sequence and model parameters.
	 * Returns a dictionary mapping each state X to an array of log probabilities, where each
	 * log probability at index i corresponds to the sum over all possible previous taggings
	 * of the probability of observation i given this state X.
	 */
	private static Map<String, Double[]> calculateForwardMatrix(List<String> observationSequence,
			Map<StringPair, Double> transLogProbDict, Map<StringPair, Double> emisLogProbDict, Set<String> stateSet,
			String startState) {
		int numObs = observationSequence.size();

		Map<String, Double[]> forwardMatrix = new HashMap<String, Double[]>();

		for (String state : stateSet) {
			forwardMatrix.put(state, new Double[numObs]);
		}

		// Begin by filling out the matrix for the first observation. This requires
		// a special case because we start out at startState with a probability of 1.0.
		for (String state : stateSet) {
			// P(state|#) * P(firstObservation|state)
			StringPair transStringPair = new StringPair(startState, state);
			Double logProbStateGivenStart = transLogProbDict.get(transStringPair);

			StringPair emisStringPair = new StringPair(state, observationSequence.get(0));
			Double logProbObsGivenState = emisLogProbDict.get(emisStringPair);

			Double logProb = StaticUtil.calcLogProductOfLogs(logProbStateGivenStart, logProbObsGivenState);
			forwardMatrix.get(state)[0] = logProb;
		}

		// Now complete the rest of the matrix.
		for (int i = 1; i < numObs; i++) {
			String obs = observationSequence.get(i);

			for (String state : stateSet) {
				Double totalLogProb = null; // Probability starts out as 0 and accumulates from all previous states.

				// The emission probability is the same given one state.
				StringPair emisStringPair = new StringPair(state, obs);
				Double logProbObsGivenState = emisLogProbDict.get(emisStringPair);

				for (String prevState : stateSet) {
					// P(state|prevState) * P(obs|state) * Forward(i-1, prevState)
					StringPair transStringPair = new StringPair(prevState, state);
					Double logProbStateGivenPrevState = transLogProbDict.get(transStringPair);

					Double prevObsPrevStateForwardLogProb = forwardMatrix.get(prevState)[i - 1];

					Double newLogProb = StaticUtil.calcLogProductOfLogs(
							StaticUtil.calcLogProductOfLogs(logProbStateGivenPrevState, logProbObsGivenState),
							prevObsPrevStateForwardLogProb);
					totalLogProb = StaticUtil.calcLogSumOfLogs(totalLogProb, newLogProb);
				}

				forwardMatrix.get(state)[i] = totalLogProb;
			}
		}

		return forwardMatrix;
	}

	/**
	 * Calculates the backward matrix given an observation sequence and model parameters.
	 * Returns a dictionary mapping each state X to an array of log probabilities, where each
	 * log probability at index i corresponds to the sum over all possible subsequent taggings
	 * given this state X, not including the observation i.
	 */
	private static Map<String, Double[]> calculateBackwardMatrix(List<String> observationSequence,
			Map<StringPair, Double> transLogProbDict, Map<StringPair, Double> emisLogProbDict, Set<String> stateSet,
			String startState) {
		int numObs = observationSequence.size();

		Map<String, Double[]> backwardMatrix = new HashMap<String, Double[]>();

		for (String state : stateSet) {
			backwardMatrix.put(state, new Double[numObs]);
		}

		// Begin by filling out the matrix for the last observation. This requires
		// a special case because we start out with a subsequent taggings probability of 1.0.
		for (String state : stateSet) {
			backwardMatrix.get(state)[numObs - 1] = Math.log(1.0) / Math.log(2);
		}

		// Now complete the rest of the matrix.
		for (int i = numObs - 2; i >= 0; i--) {
			String nextObs = observationSequence.get(i + 1);

			for (String state : stateSet) {
				Double totalLogProb = null; // Probability starts out as 0 and accumulates from all next states.

				for (String nextState : stateSet) {
					// P(nextState|state) * P(nextObs|nextState) * Backward(i+1, nextState)
					StringPair transStringPair = new StringPair(state, nextState);
					Double logProbStateGivenPrevState = transLogProbDict.get(transStringPair);

					StringPair emisStringPair = new StringPair(nextState, nextObs);
					Double logProbNextObsGivenNextState = emisLogProbDict.get(emisStringPair);

					Double nextObsNextStateBackwardLogProb = backwardMatrix.get(nextState)[i + 1];

					Double newLogProb = StaticUtil.calcLogProductOfLogs(
							StaticUtil.calcLogProductOfLogs(logProbStateGivenPrevState, logProbNextObsGivenNextState),
							nextObsNextStateBackwardLogProb);
					totalLogProb = StaticUtil.calcLogSumOfLogs(totalLogProb, newLogProb);
				}

				backwardMatrix.get(state)[i] = totalLogProb;
			}
		}

		return backwardMatrix;
	}

	/**
	 * Calculates the transition counts for an observation sequence.
	 */
	private static Map<StringPair, Double> calculateLogTransitionCounts(Map<String, Double[]> forwardMatrix,
			Map<String, Double[]> backwardMatrix, Double logAlpha, Map<StringPair, Double> transLogProbDict,
			Map<StringPair, Double> emisLogProbDict, List<String> observationSequence, Set<String> stateSet,
			String startState) {
		Map<StringPair, Double> logTransCounts = new HashMap<StringPair, Double>();

		int numObs = observationSequence.size();

		// Calculate the transition counts from #.
		for (String state : stateSet) {
			Double forwardLogProb = forwardMatrix.get(state)[0];
			Double backwardLogProb = backwardMatrix.get(state)[0];

			Double logProbStateGivenStart = StaticUtil.calcLogProductOfLogs(forwardLogProb, backwardLogProb);
			StringPair transStringPair = new StringPair(startState, state);

			if (logProbStateGivenStart != null) {
				logTransCounts.put(transStringPair, logProbStateGivenStart);
			}
		}

		// Calculate transition counts for the rest of the transitions.
		for (int i = 0; i < numObs - 1; i++) {
			String nextObs = observationSequence.get(i + 1);

			for (String fromState : stateSet) {
				Double forwardLogProb = forwardMatrix.get(fromState)[i];

				for (String toState : stateSet) {
					Double backwardLogProb = backwardMatrix.get(toState)[i + 1];

					StringPair transStringPair = new StringPair(fromState, toState);
					Double transLogProb = transLogProbDict.get(transStringPair);

					StringPair emisStringPair = new StringPair(toState, nextObs);
					Double emisLogProb = emisLogProbDict.get(emisStringPair);

					Double logProbToStateGivenFromState = StaticUtil.calcLogProductOfLogs(
							StaticUtil.calcLogProductOfLogs(
									StaticUtil.calcLogProductOfLogs(forwardLogProb, transLogProb), emisLogProb),
							backwardLogProb);
					Double prevLogTransCount = logTransCounts.get(transStringPair);
					Double sumLogTransCount = StaticUtil.calcLogSumOfLogs(prevLogTransCount,
							logProbToStateGivenFromState);

					if (sumLogTransCount != null) {
						logTransCounts.put(transStringPair, sumLogTransCount);
					}
				}
			}
		}

		// Go through and divide all counts by alpha, so that all observation sequences are weighted equally.
		for (Entry<StringPair, Double> entry : logTransCounts.entrySet()) {
			logTransCounts.put(entry.getKey(), entry.getValue() - logAlpha);
		}

		return logTransCounts;
	}

	/**
	 * Calculates the emission counts for an observation sequence.
	 */
	private static Map<StringPair, Double> calculateLogEmissionCounts(Map<String, Double[]> forwardMatrix,
			Map<String, Double[]> backwardMatrix, Double logAlpha, List<String> observationSequence,
			Set<String> stateSet) {
		Map<StringPair, Double> logEmisCounts = new HashMap<StringPair, Double>();

		int numObs = observationSequence.size();
		for (int i = 0; i < numObs; i++) {
			String obs = observationSequence.get(i);

			for (String state : stateSet) {
				Double forwardLogProb = forwardMatrix.get(state)[i];
				Double backwardLogProb = backwardMatrix.get(state)[i];

				Double logProbObsGivenState = StaticUtil.calcLogProductOfLogs(forwardLogProb, backwardLogProb);
				StringPair emisStringPair = new StringPair(state, obs);
				Double prevLogEmisCount = logEmisCounts.get(emisStringPair);
				Double sumLogEmisCount = StaticUtil.calcLogSumOfLogs(prevLogEmisCount, logProbObsGivenState);

				if (sumLogEmisCount != null) {
					logEmisCounts.put(emisStringPair, sumLogEmisCount);
				}
			}
		}

		// Go through and divide all counts by alpha, so that all observation sequences are weighted equally.
		for (Entry<StringPair, Double> entry : logEmisCounts.entrySet()) {
			logEmisCounts.put(entry.getKey(), entry.getValue() - logAlpha);
		}

		return logEmisCounts;
	}

	/**
	 * Returns the log alpha for the observation sequence under the given model, or null if alpha == 0.
	 */
	private static Double getLogAlpha(Map<String, Double[]> forwardMatrix) {
		Double logAlpha = null;

		for (Entry<String, Double[]> entry : forwardMatrix.entrySet()) {
			Double[] logProbArray = entry.getValue();
			Double lastLogProb = entry.getValue()[logProbArray.length - 1];

			logAlpha = StaticUtil.calcLogSumOfLogs(logAlpha, lastLogProb);
		}

		return logAlpha;
	}

	/**
	 * Outputs the transition log counts as EMModelParameters.
	 */
	private static void outputTransitionLogCounts(Map<StringPair, Double> transLogCounts,
			OutputCollector<Text, EMModelParameter> output) throws IOException {
		outputLogCounts(transLogCounts, output, EMModelParameter.PARAMETER_TYPE_TRANSITION);
	}

	/**
	 * Outputs the emission log counts as EMModelParameters.
	 */
	private static void outputEmissionLogCounts(Map<StringPair, Double> emisLogCounts,
			OutputCollector<Text, EMModelParameter> output) throws IOException {
		outputLogCounts(emisLogCounts, output, EMModelParameter.PARAMETER_TYPE_EMISSION);
	}

	/**
	 * Outputs the log counts as EMModelParameters.
	 */
	private static void outputLogCounts(Map<StringPair, Double> logCounts,
			OutputCollector<Text, EMModelParameter> output, char parameterType) throws IOException {
		for (Entry<StringPair, Double> entry : logCounts.entrySet()) {
			if (entry.getValue() != null) { // Only output if prob > 0.
				EMModelParameter param = new EMModelParameter(parameterType, new Text(entry.getKey().getX()), new Text(
						entry.getKey().getY()), entry.getValue());
				System.out.println("Log count: " + entry.getValue());
				System.out.println("Param: " + param);
				output.collect(param.getTransFromStateOrEmisState(), param);
			}
		}
	}

}
