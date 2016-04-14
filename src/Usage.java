/**
 * Created by marvin on 4/14/16.
 */
public class Usage {

    private int useCount;
    private double usage;

    public Usage(double usage) {

        this.usage = usage;
        useCount = 0;
    }

    public void increment() {

        useCount++;
    }

    public void update(double freshness) {

        usage = usage*(1-freshness) + freshness*useCount;
        useCount = 0;
    }

    public double getUsage() {
        return usage;
    }
}
