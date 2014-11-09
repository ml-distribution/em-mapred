package edu.dartmouth.hmmem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * Represents a single EM model parameter, such as the probability of a transition or an emission.
 * 
 * Is also used to encapsulate information about alpha of an observation sequence under a given model.
 */
public class EMModelParameter implements Writable {

	public static final char PARAMETER_TYPE_TRANSITION = 't';
	public static final char PARAMETER_TYPE_EMISSION = 'e';
	public static final char TYPE_ALPHA = 'a';

	private static final String TRANSITION_PREFIX = "Transition:";
	private static final String EMISSION_PREFIX = "Emission:";
	private static final String ALPHA_PREFIX = "Alpha:";

	public static final Text ALPHA_DUMMY_TEXT = new Text("alpha_dummy_text");

	// Indicates the type of the parameter (or alpha):
	// 't' --> transition
	// 'e' --> emission
	// 'a' --> alpha
	private char parameterType = '\0';

	private Text transFromStateOrEmisState = new Text();
	private Text transToStateOrEmisToken = new Text();

	private double logCount = 0;

	public EMModelParameter() {
	}

	public EMModelParameter(char parameterType, Text transFromStateOrEmisState, Text transToStateOrEmisToken,
			double logCount) {
		this.parameterType = parameterType;
		this.transFromStateOrEmisState = transFromStateOrEmisState;
		this.transToStateOrEmisToken = transToStateOrEmisToken;
		this.logCount = logCount;
	}

	public static EMModelParameter makeAlphaObject(double logCount) {
		return new EMModelParameter(TYPE_ALPHA, ALPHA_DUMMY_TEXT, ALPHA_DUMMY_TEXT, logCount);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		parameterType = in.readChar();

		transFromStateOrEmisState.readFields(in);
		transToStateOrEmisToken.readFields(in);

		logCount = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChar(parameterType);

		transFromStateOrEmisState.write(out);
		transToStateOrEmisToken.write(out);

		out.writeDouble(logCount);
	}

	public static EMModelParameter fromString(String str) throws Exception {
		String trimmedString = str.trim();
		if (trimmedString.length() == 0) {
			return null; // No problem, we just got an empty line.
		}

		String[] tokens = trimmedString.split("\\s+");

		char paramType;
		if (tokens[0].equals(TRANSITION_PREFIX)) {
			paramType = PARAMETER_TYPE_TRANSITION;
		} else if (tokens[0].equals(EMISSION_PREFIX)) {
			paramType = PARAMETER_TYPE_EMISSION;
		} else if (tokens[0].equals(ALPHA_PREFIX)) {
			paramType = TYPE_ALPHA;
		} else {
			StringBuilder formatExceptionSB = new StringBuilder();
			formatExceptionSB.append("Invalid format for an EMModelParameter: \"" + str + "\".");
			for (int i = 0; i < tokens.length; i++) {
				formatExceptionSB.append(" tokens[" + i + "]: \"" + tokens[i] + "\".");
			}

			throw new Exception(formatExceptionSB.toString());
		}

		EMModelParameter param;
		if (paramType == TYPE_ALPHA) {
			double logAlpha = Double.parseDouble(tokens[1]);
			param = makeAlphaObject(logAlpha);
		} else {
			Text transFromStateOrEmisState = new Text(tokens[1]);
			Text transToStateOrEmisToken = new Text(tokens[2]);
			double logCount = Double.parseDouble(tokens[3]);

			param = new EMModelParameter(paramType, transFromStateOrEmisState, transToStateOrEmisToken, logCount);
		}

		return param;
	}

	public char getParameterType() {
		return parameterType;
	}

	public void setParameterType(char parameterType) {
		this.parameterType = parameterType;
	}

	public Text getTransFromStateOrEmisState() {
		return transFromStateOrEmisState;
	}

	public void setTransFromStateOrEmisState(Text transFromStateOrEmisState) {
		this.transFromStateOrEmisState = transFromStateOrEmisState;
	}

	public Text getTransToStateOrEmisToken() {
		return transToStateOrEmisToken;
	}

	public void setTransToStateOrEmisToken(Text transToStateOrEmisToken) {
		this.transToStateOrEmisToken = transToStateOrEmisToken;
	}

	public double getLogCount() {
		return logCount;
	}

	public void setLogCount(double logCount) {
		this.logCount = logCount;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		switch (parameterType) {
		case PARAMETER_TYPE_TRANSITION:
			sb.append(TRANSITION_PREFIX);
			sb.append(" " + transFromStateOrEmisState + " " + transToStateOrEmisToken + " " + logCount);
			break;
		case PARAMETER_TYPE_EMISSION:
			sb.append(EMISSION_PREFIX);
			sb.append(" " + transFromStateOrEmisState + " " + transToStateOrEmisToken + " " + logCount);
			break;
		case TYPE_ALPHA:
			sb.append(ALPHA_PREFIX);
			sb.append(" " + logCount);
			break;
		}

		return sb.toString();
	}

}
