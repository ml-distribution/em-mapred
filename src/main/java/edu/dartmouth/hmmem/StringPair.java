package edu.dartmouth.hmmem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringPair implements Writable {

	public String getX() {
		return x;
	}

	public void setX(String x) {
		this.x = x;
	}

	public String getY() {
		return y;
	}

	public void setY(String y) {
		this.y = y;
	}

	private String x;
	private String y;

	public StringPair(String x, String y) {
		this.x = new String(x);
		this.y = new String(y);
	}

	public StringPair() {
		x = y = null;
	}

	public static StringPair stringPairFromEMModelParameter(EMModelParameter param) {
		String x = param.getTransFromStateOrEmisState().toString();
		String y = param.getTransToStateOrEmisToken().toString();

		return new StringPair(x, y);
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof StringPair)) {
			return false;
		}
		StringPair other = (StringPair) o;
		return x.equals(other.x) && y.equals(other.y);
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = hash * 17 + x.hashCode();
		hash = hash * 31 + y.hashCode();
		return hash;
	}

	@Override
	public String toString() {
		return "(" + x + "," + y + ")";
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Text xText = new Text();
		Text yText = new Text();

		xText.readFields(in);
		yText.readFields(in);

		x = xText.toString();
		y = yText.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text xText = new Text(x);
		Text yText = new Text(y);

		xText.write(out);
		yText.write(out);
	}

}
