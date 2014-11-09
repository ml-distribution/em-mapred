package exmax;

import java.util.*;

/**
 * exmax.ModelFactory contains functionality for constructing and maximizing models.
 */
public abstract class ModelFactory {

    /**
     * The delta ratio is the minimum percentage of the initial log likelihood for which no
     * further iterations will be run.
     */
    private static double deltaRatio = .001;

    /**
     * Get {@see #deltaRatio}.
     * @return {@link #deltaRatio}.
     */
    public static double getDeltaRatio() {
        return deltaRatio;
    }

    /**
     * Set {@see #deltaRatio}.
     * @param deltaRatio {@link #deltaRatio}
     */
    public static void setDeltaRatio(double deltaRatio) {
        ModelFactory.deltaRatio = deltaRatio;
    }

    /**
     * Create a exmax.Model by passing in samples and a number of components.
     *
     * @param samples the samples
     * @param componentSize the number of components, greater than or equal to 2
     * @return the generated model
     */
    public static Model createModel(List<Double> samples, int componentSize) {

        // Find the sample max & min.
        double min = Double.MAX_VALUE;
        double max = Double.MAX_VALUE * -1;
        for (int i = 0; i < samples.size(); i++) {
            min = Math.min(samples.get(i), min);
            max = Math.max(samples.get(i), max);
        }

        // Initialize the requested number of components.
        List<Component> components = new ArrayList<Component>();
        Random rand = new Random();
        for (int i = 0; i < componentSize; i++) {

            // Add a small amount of random to the mu of each component to avoid duplicates or mirror values.
            double mu = (rand.nextDouble() % 1) + min + (i + 1) * (max - min) / (componentSize + 1);

            // Initialize sigma to a width that roughly covers the sample set.
            double sigma = Math.max((max - min) / (2 * (componentSize + 1)), 1f);

            // Initialize tau to be even for each component. This won't be the case for more complex models.
            double tau = 1f / componentSize;

            Component component = new BasicComponent(mu, sigma, tau);
            components.add(component);
        }

        return new BasicModel(components, samples);
    }

    /**
     * Create a exmax.Model and then run expectation maximization on the model.
     *
     * @param samples the samples
     * @param components the number of components, greater than or equal to 2
     * @return the generated and maximized model
     */
    public static Model createMaximizedModel(List<Double> samples, int components) {
        Model model = createModel(samples, components);
        return maximize(model);
    }

    /**
     * Create a model by passing in a list of samples, starting with the simplest 2 component model,
     * maximizing the model, and then continuing to add components and maximize until adding a component
     * reduces the Bayesian Information Criterion for the model.
     *
     * @param samples the samples
     * @return the generated and maximized model that best fits the data
     */
    public static Model createMaximizedModel(List<Double> samples) {

        int i = 2;
        Model model = createMaximizedModel(samples, i);

        while (true) {
            i++;

            // Get a maximized model with 1 additional component.
            Model nextModel = createMaximizedModel(samples, i);

            // If the new model didn't improve enough to justify the component, return the current one.
            if (model.getBayesianInformationCriterion() >= nextModel.getBayesianInformationCriterion()) {
                return model;
            }

            // Otherwise keep iterating and adding components.
            model = nextModel;
        }
    }

    /**
     * Create a set of models from 2 components through the number of components specified by the
     * max model size, maximizing each model.
     *
     * @param samples the samples
     * @param modelSize the maximum number of components in a model
     * @return the list of maximized models
     */
    public static List<Model> createMaximizedModels(List<Double> samples, int modelSize) {

        List<Model> models = new ArrayList<Model>();
        for (int i = 2; i <= modelSize; i++) {
            Model model = createMaximizedModel(samples, i);
            models.add(model);
        }

        return models;
    }

    /**
     * Runs the expectation maximization algorithm on the specified model, returning a model that is at
     * least as good or better than the current model by adjusting the mu and variance of components.
     * Prior models will be stored in {@see #priorModel} as a linked list that can be walked to
     * glean information about maximization.
     *
     * @param model the initial model
     * @return the maximized model
     */
    public static Model maximize(Model model) {

        // Calculate the minimum significant delta, stop for improvement smaller than the value.
        double delta = -1 * model.getLogLikelihood() * deltaRatio;

        // Run iterations on the algorithm until we return a model.
        while (true) {

            List<Component> modelComponents = model.getComponents();
            List<Double> modelSamples = model.getSamples();
            List<Component> nextModelComponents = new ArrayList<Component>();

            for (Component component : modelComponents) {

                // Get the total relative likelihood for all samples and a given component.
                // This measure is approximately the number of samples that we expect correlate to the component.
                double totalLikelihood = 0;
                for (Double sample : modelSamples) {
                    totalLikelihood += model.getRelativeSampleLikelihood(sample, component);
                }

                // Calculate the given component mu by taking the average of the samples, weighted on correlation
                // to the component.
                double mu = 0;
                for (Double sample : modelSamples) {
                    mu += sample * model.getRelativeSampleLikelihood(sample, component) / totalLikelihood;
                }

                // Calculate the given component sigma by taking the square root of the square of the distance
                // between each sample and mu, weighted by the correlation to the component, floored at 1.
                double sigma = 0;
                for (Double sample : modelSamples) {
                    sigma += Math.pow(sample - mu, 2) * model.getRelativeSampleLikelihood(sample, component);
                }

                sigma = Math.sqrt(sigma / totalLikelihood);
                sigma = Math.max(sigma, 1.0f);

                nextModelComponents.add(new BasicComponent(mu, sigma, component.getTau()));
            }

            // Create the new model, return the current one if there isn't enough improvement.
            Model nextModel = new BasicModel(nextModelComponents, modelSamples);
            if (nextModel.getLogLikelihood() - model.getLogLikelihood() <= delta) {
                return model;
            }

            // Keep track of the prior model for back tracking, and iterate again.
            nextModel.setPriorModel(model);
            model = nextModel;
        }
    }
}
