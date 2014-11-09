package exmax;

import java.util.List;

/**
 * exmax.BasicModel provides a straightforward in memory implementation of a exmax.Model.
 */
public class BasicModel implements Model {

    /**
     * List of components in the model.
     */
    private List<Component> components;

    /**
     * List of samples in the model.
     */
    private List<Double> samples;

    /**
     * The prior model, to keep a linked list of iterated models during expectation maximization.
     */
    private Model priorModel;

    /**
     * Construct a exmax.BasicModel by passing in components and samples.
     *
     * @param components list of components in the model
     * @param samples list of samples in the model
     */
    public BasicModel(List<Component> components, List<Double> samples) {
        this.components = components;
        this.samples = samples;
        this.priorModel = null;
    }

    /**
     * @inheritDoc
     */
    @Override
    public List<Component> getComponents() {
        return components;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void setComponents(List<Component> components) {
        this.components = components;
    }

    /**
     * @inheritDoc
     */
    @Override
    public int getComponentSize() {
        return components.size();
    }

    /**
     * @inheritDoc
     */
    @Override
    public List<Double> getSamples() {
        return samples;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void setSamples(List<Double> samples) {
        this.samples = samples;
    }

    /**
     * @inheritDoc
     */
    @Override
    public int getSampleSize() {
        return samples.size();
    }

    /**
     * @inheritDoc
     */
    @Override
    public Model getPriorModel() {
        return priorModel;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void setPriorModel(Model priorModel) {
        this.priorModel = priorModel;
    }

    /**
     * @inheritDoc
     */
    @Override
    public double getRelativeSampleLikelihood(Double sample, Component component) {

        double t = 0;
        for (Component c : getComponents()) {
            t += c.getSampleLikelihood(sample) * c.getTau();
        }

        double l = component.getSampleLikelihood(sample) * component.getTau() / t;
        if (Double.isNaN(l)) {
            return 0;
        }

        return l;
    }

    /**
     * @inheritDoc
     */
    @Override
    public double getLogLikelihood() {

        double l = 0;
        for (Double sample : getSamples()) {
            double c = 1;
            for (Component component : getComponents()) {
                c *= Math.pow(component.getTau() * component.getSampleLikelihood(sample),
                        getRelativeSampleLikelihood(sample, component));
            }

            l += Math.log(c);
        }

        return l;
    }

    /**
     * @inheritDoc
     */
    @Override
    public double getBayesianInformationCriterion() {
        return 2 * getLogLikelihood() - (getComponentSize() * Math.log(getSampleSize()));
    }

    /**
     * @inheritDoc
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("================================================" + System.lineSeparator());
        sb.append("Components: " + getComponentSize() + System.lineSeparator());
        sb.append("Log Likelihood: " + String.format("%.3f", getLogLikelihood()) + System.lineSeparator());
        sb.append("BIC: " + String.format("%.3f", getBayesianInformationCriterion()) + System.lineSeparator());
        sb.append("------------------------------------------------" + System.lineSeparator());
        sb.append("Final Components:" + System.lineSeparator());

        int i = 1;
        for (Component component : getComponents()) {
            sb.append(i + ". Mu=" + String.format("%.3f", component.getMu()) + " Sigma=" +
                    String.format("%.3f", component.getSigma()) + System.lineSeparator());
            i++;
        }

        sb.append("------------------------------------------------" + System.lineSeparator());
        sb.append("Iterations:" + System.lineSeparator());

        int j = 1;
        Model model = this;
        while (model.getPriorModel() != null) {
            model = model.getPriorModel();
            j++;
        }


        model = this;
        while (j > 0) {
            sb.append(j + ". ");
            int k = 1;
            for (Component component : model.getComponents()) {
                sb.append("Mu" + k + "=" + String.format("%.1f", component.getMu()) + " ");
                k++;
            }
            sb.append("Lk=" + String.format("%.3f",model.getLogLikelihood()) + System.lineSeparator());

            model = model.getPriorModel();
            j--;
        }

        return sb.toString();
    }
}
