package zx.soft.em.simple1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author wgybzb
 *
 */
public class ExpectationMaximization {

	private Parameters parameters;
	private CoinToss lambdaType;
	private List<Observation> observationList;
	private final Map<CoinToss, Float> lambdaCountTable;

	//    H T
	//  H
	//  T
	//
	private final Float[][] betaCountTable = new Float[2][2];

	public ExpectationMaximization() {
		lambdaCountTable = new HashMap<>();
	}

	public void stepE() {

		Map<Observation, Float> obsToProbMap = new HashMap<>();

		//build lambda h and t count table
		float count = 0;

		for (Observation observation : observationList) {

			float prob = getProbability(lambdaType, observation);

			obsToProbMap.put(observation, prob);

			count += prob;
		}

		float oppositeCount = 0;

		for (Observation observation : observationList) {

			float prob = (1 - getProbability(lambdaType, observation));

			oppositeCount += prob;
		}

		lambdaCountTable.put(lambdaType, count);
		lambdaCountTable.put(parameters.getOppositeType(lambdaType), oppositeCount);

		//build hh, ht, th, tt count table

		float hh = 0;

		for (Map.Entry<Observation, Float> entry : obsToProbMap.entrySet()) {

			int frequency = getFrequency(lambdaType, entry.getKey());

			hh += entry.getValue() * frequency;

		}

		betaCountTable[0][0] = hh;

		float ht = 0;

		for (Map.Entry<Observation, Float> entry : obsToProbMap.entrySet()) {

			int frequency = getFrequency(parameters.getOppositeType(lambdaType), entry.getKey());

			ht += entry.getValue() * frequency;

		}

		betaCountTable[0][1] = ht;

		float th = 0;

		for (Map.Entry<Observation, Float> entry : obsToProbMap.entrySet()) {

			int frequency = getFrequency(lambdaType, entry.getKey());

			th += (1 - entry.getValue()) * frequency;

		}

		betaCountTable[1][0] = th;

		float tt = 0;

		for (Map.Entry<Observation, Float> entry : obsToProbMap.entrySet()) {

			int frequency = getFrequency(parameters.getOppositeType(lambdaType), entry.getKey());

			tt += (1 - entry.getValue()) * frequency;

		}

		betaCountTable[1][1] = tt;

		System.out.println("probabilities: " + obsToProbMap.toString() + "\n");

	}

	public int getFrequency(CoinToss type, Observation observation) {
		int frequency = 0;

		if (observation.coinTossOne == type)
			frequency++;
		if (observation.coinTossTwo == type)
			frequency++;
		if (observation.coinTossThree == type)
			frequency++;

		return frequency;

	}

	public void stepM() {

		//calculate new lambda
		parameters.lambda = lambdaCountTable.get(lambdaType) / observationList.size();

		//build new betaOne from table
		parameters.betaOne = betaCountTable[0][0] / (betaCountTable[0][0] + betaCountTable[0][1]);

		//build new betaTwo from table
		parameters.betaTwo = betaCountTable[1][0] / (betaCountTable[1][0] + betaCountTable[1][1]);

	}

	public float getProbability(CoinToss lambdaType, Observation observation) {

		float numerator = numeratorProduct(lambdaType, observation);

		float denominator = denominatorProduct(lambdaType, observation);

		return numerator / denominator;
	}

	private float numeratorProduct(CoinToss lambdaType, Observation observation) {
		float lambda = parameters.getLambdaValue(lambdaType);

		float betaProduct = parameters.getBetaValue(lambdaType, observation.coinTossOne)
				* parameters.getBetaValue(lambdaType, observation.coinTossTwo)
				* parameters.getBetaValue(lambdaType, observation.coinTossThree);

		return lambda * betaProduct;
	}

	private float denominatorProduct(CoinToss lambdaType, Observation observation) {

		float firstProduct = numeratorProduct(lambdaType, observation);

		float OppositeLambdaValue = parameters.getOppositeLambdaValue(lambdaType);

		CoinToss lambdaOpposite = parameters.getOppositeType(lambdaType);

		float OppositeBetaFirstToss = parameters.getBetaValue(lambdaOpposite, observation.coinTossOne);
		float OppositeBetaSecondToss = parameters.getBetaValue(lambdaOpposite, observation.coinTossTwo);
		float OppositeBetaThirdToss = parameters.getBetaValue(lambdaOpposite, observation.coinTossThree);

		float secondProduct = OppositeLambdaValue
				* (OppositeBetaFirstToss * OppositeBetaSecondToss * OppositeBetaThirdToss);

		return firstProduct + secondProduct;
	}

	public void computeEM(CoinToss lambda_type, Parameters param, List<Observation> observations) {

		this.lambdaType = lambda_type;
		this.parameters = param;
		this.observationList = observations;

		boolean done = false;
		int iteration = 0;
		float logSum = 0;

		while (!done) {

			float newLogSum = 0;

			for (Observation observation : this.observationList) {
				newLogSum += getLogSum(observation);
			}

			//print stuff
			System.out.println("iteration " + iteration++ + "\n" + parameters.toString() + "\nlogsum: " + newLogSum);

			stepE();

			stepM();

			if (newLogSum == logSum)
				done = true;

			logSum = newLogSum;

			if (this.parameters.betaOne == 1 || this.parameters.betaTwo == 1 || this.parameters.betaOne == 0
					|| this.parameters.betaTwo == 0) {
				done = true;
			}

		}
	}

	public float getLogSum(Observation observation) {
		int frequencyHeads = getFrequency(lambdaType, observation);
		int frequencyTails = getFrequency(parameters.getOppositeType(lambdaType), observation);

		float term = (float) (((parameters.lambda * Math.pow(parameters.betaOne, frequencyHeads)) * Math.pow(
				(1 - parameters.betaOne), frequencyTails)) + ((1 - parameters.lambda)
				* Math.pow(parameters.betaTwo, frequencyHeads) * Math.pow((1 - parameters.betaTwo), frequencyTails)));

		return (float) Math.log(term);
	}

	public static void main(String[] args) {
		ExpectationMaximization EM = new ExpectationMaximization();
		Parameters param = new Parameters();
		CoinToss lambdaType = CoinToss.HEADS;
		ArrayList<Observation> observations = new ArrayList<>();

		for (int i = 0; i < 2; i++) {

			Observation observation = new Observation();

			observation.coinTossOne = CoinToss.TAILS;
			observation.coinTossTwo = CoinToss.HEADS;
			observation.coinTossThree = CoinToss.TAILS;

			observations.add(observation);
		}

		for (int i = 0; i < 3; i++) {

			Observation observation = new Observation();

			observation.coinTossOne = CoinToss.TAILS;
			observation.coinTossTwo = CoinToss.HEADS;
			observation.coinTossThree = CoinToss.HEADS;

			observations.add(observation);
		}

		param.lambda = 0.3F;
		param.betaOne = 0.3F;
		param.betaTwo = 0.6F;

		EM.computeEM(lambdaType, param, observations);

	}

}
