package edu.dartmouth.hmmem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class TaggedObservationSequence implements Writable {

	private final List<StringPair> obsTags = new LinkedList<>();
	private LongWritable byteOffset = new LongWritable();

	public void setByteOffset(LongWritable byteOffset) {
		this.byteOffset = byteOffset;
	}

	public void setByteOffset(long byteOffset) {
		this.byteOffset = new LongWritable(byteOffset);
	}

	public void appendObsTag(StringPair obsTag) {
		obsTags.add(obsTag);
	}

	public void prependObsTag(StringPair obsTag) {
		obsTags.add(0, obsTag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		byteOffset.readFields(in);

		int length = in.readInt();

		for (int i = 0; i < length; i++) {
			StringPair obsTag = new StringPair();
			obsTag.readFields(in);
			obsTags.add(obsTag);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byteOffset.write(out);

		int length = obsTags.size();
		out.writeInt(length);

		for (StringPair obsTag : obsTags) {
			obsTag.write(out);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append(byteOffset.toString() + ": ");

		for (StringPair obsTag : obsTags) {
			sb.append(obsTag.toString() + " ");
		}

		return sb.toString();
	}

}
