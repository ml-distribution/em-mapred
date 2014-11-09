package exmax;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

	/**
	 * exmax.Main thread of execution, for generating simple models based on samples that come from
	 * a text file and writing information about the models to another text file.
	 *
	 * @param args the path of the input file and the output file
	 */
	public static void main(String[] args) {

		try {

			System.out.println("================================================");
			System.out.println("ExMax: A tool for expectation maximization.");

			// If args weren't specified, print usage and exit.
			if (args.length != 2) {
				printUsage();
				System.exit(-1);
			}

			// Otherwise parse args.
			String inputPath = args[0];
			String outputPath = args[1];

			// Load samples.
			List<Double> samples = loadSamples(inputPath);

			// Create models with 2-5 components.
			List<Model> models = ModelFactory.createMaximizedModels(samples, 5);

			// Create the best model for the samples.
			//List<exmax.Model> models = new ArrayList<exmax.Model>();
			//exmax.Model model = exmax.ModelFactory.createMaximizedModel(samples);
			//models.add(model);

			// Write the models to file.
			writeModels(outputPath, models);

		} catch (Exception ex) {

			// If there was an exception, print it and exit with negative status code.
			ex.printStackTrace();
			System.exit(-1);
		}

		System.exit(0);
	}

	/**
	 * Print typical command line usage information.
	 */
	private static void printUsage() {

		System.out.println("================================================");
		System.out.println("Usage: <exmax> [inputfile] [outputfile]");
		System.out.println("Example: exmax data/sample1.txt data/output.txt");
	}

	/**
	 * Load samples from a text file, where the samples are space delimited.
	 *
	 * @param filePath the path of the input file
	 * @return a list of samples, represented as doubles
	 * @throws IOException
	 */
	public static List<Double> loadSamples(String filePath) throws IOException {

		List<Double> samples = new ArrayList<Double>();
		FileReader fr = new FileReader(filePath);
		BufferedReader br = new BufferedReader(fr);
		String line;

		while ((line = br.readLine()) != null) {
			for (String s : line.split("\\s")) {
				try {
					double d = Double.valueOf(s);
					samples.add(d);
				}

				catch (Exception ex) {
				}
			}
		}
		br.close();

		return samples;
	}

	/**
	 * Write a list of models to a text file, in a convenient format for humans to parse.
	 *
	 * @param filePath the path of the output file
	 * @param models the list of models to write
	 * @throws IOException
	 */
	public static void writeModels(String filePath, List<Model> models) throws IOException {

		FileWriter fw = new FileWriter(filePath);
		BufferedWriter bw = new BufferedWriter(fw);

		for (Model model : models) {
			System.out.println(model.toString());
			bw.write(model.toString());
		}

		bw.close();
	}
}
