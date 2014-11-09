package exmax;

/**
 * exmax.BasicComponent provides a straightforward in memory implementation of a exmax.Component.
 */
public class BasicComponent implements Component {

    /**
     * The mu value, or media of the component.
     */
    private double mu;

    /**
     * The sigma value, or variance of the component.
     */
    private double sigma;

    /**
     * The tau value, or mixing proportion of the component.
     */
    private double tau;

    /**
     * Construct a exmax.BasicComponent by passing a mu, sigma, and tau.
     *
     * @param mu the mu value
     * @param sigma the sigma value
     * @param tau the tau value
     */
    public BasicComponent(double mu, double sigma, double tau) {
        this.mu = mu;
        this.sigma = sigma;
        this.tau = tau;
    }

    /**
     * @inheritDoc
     */
    @Override
    public double getMu() {
        return mu;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void setMu(float mu) {
        this.mu = mu;
    }

    /**
     * @inheritDoc
     */
    @Override
    public double getSigma() {
        return sigma;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void setSigma(float sigma) {
        this.sigma = sigma;
    }

    /**
     * @inheritDoc
     */
    @Override
    public double getTau() {
        return tau;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void setTau(float tau) {
        this.tau = tau;
    }

    /**
     * @inheritDoc
     */
    @Override
    public double getSampleLikelihood(Double sample) {

        double l = Math.exp(-1 * Math.pow(sample - mu, 2) / (2 * sigma)) / Math.sqrt(2 * Math.PI * sigma);

        if (Double.isNaN(l)) {
            return Double.MIN_VALUE;
        }

        return l + Double.MIN_VALUE;
    }
}
