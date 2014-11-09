package zx.soft.em.simple1;

public class LogSum {

	public LogSum() {
		//
	}

	public static double logAdd(double logX, double logY) {

		// 1. make X the max
		if (logY > logX) {
			double temp = logX;
			logX = logY;
			logY = temp;
		}

		// 2. now X is bigger
		if (logX == Double.NEGATIVE_INFINITY) {
			return logX;
		}

		// 3. how far "down" (think decibels) is logY from logX?
		//    if it's really small (20 orders of magnitude smaller), then ignore
		double negDiff = logY - logX;
		if (negDiff < -20) {
			return logX;
		}

		// 4. otherwise use some nice algebra to stay in the log domain
		//    (except for negDiff)
		return logX + java.lang.Math.log(1.0 + java.lang.Math.exp(negDiff));
	}

}
