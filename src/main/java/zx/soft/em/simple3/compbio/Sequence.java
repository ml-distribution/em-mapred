package zx.soft.em.simple3.compbio;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;

public class Sequence {

	private final static int WIDTH = 80;

	private final String header;

	private final String body;

	/*
	 * Reads the first sequence from the specified file
	 */
	public Sequence(String filename) throws IOException {
		Scanner in = new Scanner(new File(filename));
		header = in.nextLine();
		StringBuffer buffer = new StringBuffer();
		while (in.hasNextLine()) {
			String line = in.nextLine().trim();
			if (line.charAt(0) == '>')
				break;
			buffer.append(line);
		}
		body = buffer.toString();
		in.close();
	}

	/*
	 * Constructor from data
	 */
	public Sequence(String header, String body) {
		this.header = header;
		this.body = body;
	}

	/*
	 * Returns the length of the sequence
	 */
	public int length() {
		return body.length();
	}

	/*
	 * Returns a String representation of the file
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(header);
		sb.append('\n');
		int i = 0;
		for (i = 0; i < body.length() - WIDTH; i += WIDTH) {
			sb.append(body.substring(i, i + WIDTH));
			sb.append('\n');
		}
		sb.append(body.substring(i));
		sb.append('\n');
		return sb.toString();
	}

	public String getHeader() {
		return header;
	}

	public String getBody() {
		return body;
	}

	public int[] getCounts() {
		int[] counts = new int[5];
		for (int i = 0; i < body.length(); i++) {
			counts[getIndex(body.charAt(i))]++;
		}
		return counts;
	}

	private static int getIndex(char c) {
		switch (c) {
		case 'A':
		case 'a':
			return 0;
		case 'C':
		case 'c':
			return 1;
		case 'G':
		case 'g':
			return 2;
		case 'T':
		case 't':
			return 3;
		default:
			return 4;
		}
	}

	public Sequence getSubsequence(int start, int end, boolean positive) {
		String subseq = body.substring(start, end);
		Sequence s = new Sequence(header + "(" + start + "-" + end + ")", subseq);
		if (positive)
			return s;
		else
			return s.reverseComplement();
	}

	public Sequence reverseComplement() {
		StringBuffer sb = new StringBuffer();
		for (int i = body.length() - 1; i >= 0; i--) {
			sb.append(complement(body.charAt(i)));
		}
		return new Sequence(header + " (reverse)", sb.toString());
	}

	public static char complement(char c) {
		switch (c) {
		case 'A':
			return 'T';
		case 'C':
			return 'G';
		case 'G':
			return 'C';
		case 'T':
			return 'A';
		default:
			return 'N';

		}
	}

	/*
	 * Returns all sequences from a FASTA file
	 */
	public static Vector<Sequence> getSequencesFromFile(String filename) throws IOException {
		Vector<Sequence> sequences = new Vector<Sequence>();
		Scanner in = new Scanner(new File(filename));
		String head = null;
		StringBuffer body = new StringBuffer();
		while (in.hasNextLine()) {
			String line = in.nextLine();
			if (line.charAt(0) == '>') {
				if (head != null)
					sequences.add(new Sequence(head, body.toString()));
				head = line;
				body = new StringBuffer();
			} else
				body.append(line);
		}
		if (head != null)
			sequences.add(new Sequence(head, body.toString()));
		in.close();
		return sequences;
	}

}
