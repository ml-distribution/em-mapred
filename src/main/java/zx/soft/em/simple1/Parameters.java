package zx.soft.em.simple1;

/**
 * 
 * @author wgybzb
 *
 */
public class Parameters {

	public float lambda;
	public float betaOne;
	public float betaTwo;

	public float getLambdaValue(CoinToss coinToss) {

		switch (coinToss) {
		case HEADS:
			return lambda;
		case TAILS:
			return (1 - lambda);
		default:
			return -1;
		}
	}

	public float getOppositeLambdaValue(CoinToss coinToss) {

		switch (coinToss) {
		case HEADS:
			return (1 - lambda);
		case TAILS:
			return lambda;
		default:
			return -1;
		}
	}

	public CoinToss getOppositeType(CoinToss coinToss) {
		if (coinToss == CoinToss.HEADS) {
			return CoinToss.TAILS;
		} else
			return CoinToss.HEADS;
	}

	public float getBetaValue(CoinToss lambdaType, CoinToss betaType) {
		if (lambdaType == CoinToss.HEADS) {
			return getBetaOneValue(betaType);
		} else
			return getBetaTwoValue(betaType);
	}

	private float getBetaOneValue(CoinToss coinToss) {

		switch (coinToss) {
		case HEADS:
			return betaOne;
		case TAILS:
			return (1 - betaOne);
		default:
			return -1;
		}
	}

	private float getBetaTwoValue(CoinToss coinToss) {

		switch (coinToss) {
		case HEADS:
			return betaTwo;
		case TAILS:
			return (1 - betaTwo);
		default:
			return -1;
		}
	}

	@Override
	public String toString() {
		return "lambda: " + lambda + "\nbetaOne: " + betaOne + "\nbetaTwo: " + betaTwo;
	}

}
