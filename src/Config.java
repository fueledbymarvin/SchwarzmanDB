import java.util.ArrayList;
import java.util.List;

/**
 * Created by marvin on 4/25/16.
 */
public class Config {

    private static int DEFAULT_PERIOD = 100;
    private static double DEFAULT_FRESHNESS = 0.25;
    private static double DEFAULT_CREATE_THRESHOLD = 1.2;
    private static double DEFAULT_DESTROY_THRESHOLD = 0.8;

    public static Config defaultConfig() {

        return new Config(DEFAULT_PERIOD, DEFAULT_FRESHNESS, DEFAULT_CREATE_THRESHOLD, DEFAULT_DESTROY_THRESHOLD);
    }

    private int period; // number of queries to process before update
    private double freshness; // proportion of overall usage that the latest measurement counts for
    private double createThreshold, destroyThreshold;

    public Config(int period, double freshness, double createThreshold, double destroyThreshold) {
        this.period = period;
        this.freshness = freshness;
        this.createThreshold = createThreshold;
        this.destroyThreshold = destroyThreshold;
    }

    public Config(String configStr) {

        List<String> config = CSV.split(configStr, ",");
        period = Integer.parseInt(config.get(0));
        freshness = Double.parseDouble(config.get(1));
        createThreshold = Double.parseDouble(config.get(2));
        destroyThreshold = Double.parseDouble(config.get(3));
    }

    public int getPeriod() {
        return period;
    }

    public double getFreshness() {
        return freshness;
    }

    public double getCreateThreshold() {
        return createThreshold;
    }

    public double getDestroyThreshold() {
        return destroyThreshold;
    }

    @Override
    public String toString() {

        List<String> vals = new ArrayList<>();
        vals.add(String.valueOf(period));
        vals.add(String.valueOf(freshness));
        vals.add(String.valueOf(createThreshold));
        vals.add(String.valueOf(destroyThreshold));
        return CSV.join(vals, ",");
    }
}
