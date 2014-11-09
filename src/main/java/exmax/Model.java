package exmax;

import java.util.List;

/**
 * exmax.Model represents a distribution of samples and a set of components that attempt to describe the
 * samples as accurately as possible.
 */
public interface Model {

    /**
     * Gets the list of components in the model.
     *
     * @return list of components in the model
     */
    List<Component> getComponents();

    /**
     * Sets the list of components in the model.
     *
     * @return list of components in the model
     */
    void setComponents(List<Component> components);

    /**
     * Gets the number of components in the model.
     *
     * @return the number of components in the model
     */
    int getComponentSize();

    /**
     * Gets the list of samples in the model.
     *
     * @return the list of samples in the model
     */
    List<Double> getSamples();

    /**
     * Gets the number of samples in the model.
     *
      * @return the number of samples in the model
     */
    int getSampleSize();

    /**
     * Sets the list of samples in the model.
     *
     * @param samples the list of samples in the model
     */
    void setSamples(List<Double> samples);

    /**
     * Gets the relative likelihood of a sample occurring in the specified component by comparing
     * the component likelihood with the likelihood of each other component in the model.
     *
     * @param sample the sample of interest for the respective likelihood
     * @param component the component of interest for the respective likelihood
     * @return the likelihood that the sample is in the specified component, from 0-1
     */
    double getRelativeSampleLikelihood(Double sample, Component component);

    /**
     * Gets the logarithm of the likelihood of the entire model, given the samples.  The value will
     * become a larger negative number as additional samples are added.
     *
     * @return the logarithm of the likelihood as a negative value where closer to zero is better
     */
    double getLogLikelihood();

    /**
     * Gets the BIC, a measure of how well the model fits the data with a weighted penalty for
     * adding additional components to avoid over-fitting.
     *
     * @return the BIC as a negative value where closer to zero is better
     */
    double getBayesianInformationCriterion();

    /**
     * Gets the model from the previous iteration of expectation maximization, or null if either no
     * model exists or the algorithm hasn't been run.
     *
     * @return the model from the previous iteration of expectation maximization
     */
    Model getPriorModel();

    void setPriorModel(Model priorModel);
}
