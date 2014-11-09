package exmax;

/**
 * exmax.Component represents a single contributing factor (often a hidden input) to a distribution of samples.
 */
public interface Component {

    /**
     * Gets the mu value, which is the median of the component.
     *
     * @return the mu value
     */
    double getMu();

    /**
     * Sets the mu value, which is the median of the component.
     * @param mu the mu value
     */
    void setMu(float mu);

    /**
     * Gets the sigma value, which is the variance of the component.
     * @return the sigma value
     */
    double getSigma();

    /**
     * Gets the sigma value, which is the variance of the component.
     * @param sigma the sigma value
     */
    void setSigma(float sigma);

    /**
     * Gets the tau value, which is the mixing proportion of the component. The tau values for all of
     * the components in the model must sum to 1.
     *
     * @return the tau value, between 0 and 1
     */
    double getTau();

    /**
     * Sets the tau value, which is the mixing proportion of the component. The tau values for all of
     * the components in the model must sum to 1.
     *
     * @param tau the tau value, between 0 and 1
     */
    void setTau(float tau);

    /**
     * Gets the likelihood of a sample occurring in the single component, which is equivalent to the function
     * that defines the component plugging in the sample value.
     *
     * @param sample the sample value
     * @return the likelihood of the sample occurring in the component, between the smallest double value and 1
     */
    double getSampleLikelihood(Double sample);

}
