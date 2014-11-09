package edu.dartmouth.hmmem;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * 静态工具类，包含了分布式HMM-EM的相关方法
 */
public class StaticUtil {

	/**
	 * Given two numbers log(x) and log(y), returns log(x+y). If both log(x) and log(y) are negative,
	 * calculates the log of the sum in the following manner to reduce the risk of underflow:
	 * Finds log(z) = min(log(x), log(y)). Then adds log(z) to both log(x) and log(y) before
	 * exponentiating, adds the two exponentiations, takes the log of the sum, and then
	 * subtracts log(z). This calculation works properly because z*x + z*y = z*(x+y),
	 * so 2^(log(z)+log(x)) + 2^(log(z)+log(y)) = 2^(log(z)+log(x+y)).
	 * Thus, log(2^(log(z)+log(x)) + 2^(log(z)+log(y))) = log(2^(log(z)+log(x+y))) = log(z)+log(x+y).
	 *
	 * Note: If logx (logy) is passed as null, then x (y) is interpreted as 0. If both parameters
	 * are passed as null, then null is returned.
	 */
	public static Double calcLogSumOfLogs(Double logX, Double logY) {
		if (logX == null && logY == null) {
			return null;
		} else if (logX == null) {
			return logY;
		} else if (logY == null) {
			return logX;
		}

		if (logX >= 0 || logY >= 0) {
			double x = Math.pow(2, logX);
			double y = Math.pow(2, logY);

			double logSum = Math.log(x + y) / Math.log(2);
			return logSum;
		}

		double logZ = Math.max(logX, logY) * -1.0;
		double scaledLogX = logX + logZ;
		double scaledLogY = logY + logZ;

		double scaledX = Math.pow(2, scaledLogX);
		double scaledY = Math.pow(2, scaledLogY);

		double logScaledSum = Math.log(scaledX + scaledY) / Math.log(2);
		double logSum = logScaledSum - logZ;

		if (!(Double.isNaN(logX) || Double.isNaN(logY))) {
			System.out.println("logX = " + logX + ", logY = " + logY + ", logSum = " + logSum);
		}

		return logSum;
	}

	/**
	 * Given two numbers log(x) and log(y), returns log(x*y) = log(x) + log(y). This method is used
	 * because it handles the case where x or y is 0, which is represented by log(x) or log(y) being null.
	 * In this case, x*y == 0, so null is returned to represent log(0).
	 */
	public static Double calcLogProductOfLogs(Double logX, Double logY) {
		if (logX == null || logY == null) {
			return null;
		}

		return logX + logY;
	}

	/**
	 * Normalizes a log probability map such that all the probabilities
	 * where the first string in the string pair key is some string x sum to 1.0.
	 */
	public static void normalizeLogProbMap(Map<StringPair, Double> logProbMap) {
		Map<String, Double> logProbSumMap = new HashMap<>();

		// See how much we have to scale down by to normalize.
		for (StringPair stringPair : logProbMap.keySet()) {
			String x = stringPair.getX();

			Double logProb = logProbMap.get(stringPair);
			Double prevLogProbSum = logProbSumMap.get(x);

			Double newLogProbSum = calcLogSumOfLogs(logProb, prevLogProbSum);
			logProbSumMap.put(x, newLogProbSum);
		}

		// Do the normalization.
		for (StringPair stringPair : logProbMap.keySet()) {
			Double prevLogProb = logProbMap.get(stringPair);
			Double logProbSum = logProbSumMap.get(stringPair.getX());

			if (prevLogProb != null) {
				Double normLogProb = prevLogProb - logProbSum;
				logProbMap.put(stringPair, normLogProb);
			}
		}
	}

	public static Set<String> makeStateSetFromTransDict(Map<StringPair, Double> transDict) {
		Set<String> stateSet = new HashSet<String>();

		for (Entry<StringPair, Double> entry : transDict.entrySet()) {
			// Add the to state for each transition.
			stateSet.add(entry.getKey().getY());
		}

		return stateSet;
	}

	/**
	 * Reads the given model parameters file and fills in the transition and emission
	 * log probabilities maps.
	 */
	public static void readModelParametersFile(FSDataInputStream in, Map<StringPair, Double> transLogProbMap,
			Map<StringPair, Double> emisLogProbMap) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		String line;
		while (null != (line = reader.readLine())) {
			EMModelParameter param = EMModelParameter.fromString(line);

			if (param != null) {
				switch (param.getParameterType()) {
				case EMModelParameter.PARAMETER_TYPE_TRANSITION:
					StringPair fromStateToState = new StringPair(param.getTransFromStateOrEmisState().toString(), param
							.getTransToStateOrEmisToken().toString());

					if (transLogProbMap.containsKey(fromStateToState)) {
						throw new Exception("Transition " + fromStateToState
								+ " was read twice in EM model parameter files.");
					}

					transLogProbMap.put(fromStateToState, param.getLogCount());
					break;
				case EMModelParameter.PARAMETER_TYPE_EMISSION:
					StringPair stateToken = new StringPair(param.getTransFromStateOrEmisState().toString(), param
							.getTransToStateOrEmisToken().toString());

					if (emisLogProbMap.containsKey(stateToken)) {
						throw new Exception("Emission " + stateToken + " was read twice in EM model parameter files.");
					}

					emisLogProbMap.put(stateToken, param.getLogCount());
					break;
				}
			}
		}

	}

}
