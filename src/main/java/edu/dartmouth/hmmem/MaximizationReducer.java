package edu.dartmouth.hmmem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reducer to perform maximization step.
 * Input - 
 * key: state (either start of transition, state for emission, or dummy state for alpha)
 * value: EMModelParameter object representing transition, emission, or alpha dummy with log counts
 * 
 * Output -
 * key: state (either start of transition or state for emission, but no dummy state for alpha)
 * value: EMModelParameter object
 * 
 * The reducer works by taking the expected log counts for transitionLogCounts that start on the
 * given state (i.e. the input key) or emissionLogCounts for the state and normalizing (sum of probabilities == 1) to
 * get new model parameters. 
 *
 * The reducer that receives the alphas from the mappers will multiply them together to produce
 * total alpha and will output this to a specially named file in the output file path directory.
 */

public class MaximizationReducer extends MapReduceBase implements
		Reducer<Text, EMModelParameter, NullWritable, EMModelParameter> {

	public static final String TOTAL_LOG_ALPHA_FILE_NAME = "total_log_alpha.txt";

	private String outputPathStr;
	private URI bucketURI;

	private boolean failure = false;
	private String failureString;

	@Override
	public void reduce(Text key, Iterator<EMModelParameter> expectedCounts,
			OutputCollector<NullWritable, EMModelParameter> output, Reporter reporter) throws IOException {
		if (failure) {
			throw new IOException(failureString);
		}

		Map<StringPair, Double> transLogCounts = new HashMap<StringPair, Double>();
		Map<StringPair, Double> emisLogCounts = new HashMap<StringPair, Double>();

		Double totalLogAlpha = 0.0;
		boolean alphaOutput = false;

		// Aggregate the counts.
		while (expectedCounts.hasNext()) {
			EMModelParameter expectedCount = expectedCounts.next();
			switch (expectedCount.getParameterType()) {
			case EMModelParameter.PARAMETER_TYPE_TRANSITION:
				transLogCounts.put(StringPair.stringPairFromEMModelParameter(expectedCount),
						expectedCount.getLogCount());
				break;
			case EMModelParameter.PARAMETER_TYPE_EMISSION:
				emisLogCounts
						.put(StringPair.stringPairFromEMModelParameter(expectedCount), expectedCount.getLogCount());
				break;
			case EMModelParameter.TYPE_ALPHA:
				totalLogAlpha = StaticUtil.calcLogProductOfLogs(totalLogAlpha, expectedCount.getLogCount());
				alphaOutput = true;
				break;
			}
		}

		// Normalize the counts to get the new model probabilities.
		StaticUtil.normalizeLogProbMap(transLogCounts);
		StaticUtil.normalizeLogProbMap(emisLogCounts);

		// Output the new model parameters to be used by the next iteration or the final model.
		outputTransitionLogCounts(transLogCounts, output);
		outputEmissionLogCounts(emisLogCounts, output);

		// Output the total log alpha if appropriate.
		if (alphaOutput) {
			String totalLogAlphaPathStr = outputPathStr + "/" + TOTAL_LOG_ALPHA_FILE_NAME;
			Path totalLogAlphaPath = new Path(totalLogAlphaPathStr);

			FileSystem fs = NativeS3FileSystem.get(bucketURI, new Configuration());
			FSDataOutputStream totalLogAlphaOut = fs.create(totalLogAlphaPath, false);

			EMModelParameter totalLogAlphaObject = EMModelParameter.makeAlphaObject(totalLogAlpha);
			totalLogAlphaOut.write(totalLogAlphaObject.toString().getBytes());

			totalLogAlphaOut.close();
		}
	}

	@Override
	public void configure(JobConf job) {
		super.configure(job);

		outputPathStr = FileOutputFormat.getOutputPath(job).toString();

		try {
			bucketURI = new URI(job.get(ExpectationMapper.BUCKET_URI_KEY));
		} catch (URISyntaxException e) {
			failure = true;
			failureString = e.toString();
		}
	}

	/**
	 * Outputs the transition log counts as EMModelParameters.
	 */
	private static void outputTransitionLogCounts(Map<StringPair, Double> transLogCounts,
			OutputCollector<NullWritable, EMModelParameter> output) throws IOException {
		outputLogCounts(transLogCounts, output, EMModelParameter.PARAMETER_TYPE_TRANSITION);
	}

	/**
	 * Outputs the emission log counts as EMModelParameters.
	 */
	private static void outputEmissionLogCounts(Map<StringPair, Double> emisLogCounts,
			OutputCollector<NullWritable, EMModelParameter> output) throws IOException {
		outputLogCounts(emisLogCounts, output, EMModelParameter.PARAMETER_TYPE_EMISSION);
	}

	/**
	 * Outputs the log counts as EMModelParameters.
	 */
	private static void outputLogCounts(Map<StringPair, Double> logCounts,
			OutputCollector<NullWritable, EMModelParameter> output, char parameterType) throws IOException {
		for (Entry<StringPair, Double> entry : logCounts.entrySet()) {
			if (entry.getValue() != null) { // Only output if prob > 0.
				EMModelParameter param = new EMModelParameter(parameterType, new Text(entry.getKey().getX()), new Text(
						entry.getKey().getY()), entry.getValue());
				System.out.println("Log count: " + entry.getValue());
				System.out.println("Param: " + param);
				output.collect(NullWritable.get(), param);
			}
		}
	}

}