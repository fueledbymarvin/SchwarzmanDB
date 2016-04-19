import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by marvin on 4/14/16.
 */
public class TableUsage {

    private int queries; // number of queries since last measurement
    private int period; // number of queries to process before update
    private double freshness; // proportion of overall usage that the latest measurement counts for
    private double maxUsage; // maximum usage of all columns
    private double primaryThreshold; // need to have usage > primaryThreshold*maxUsage to go from secondary to primary
    private double secondaryThreshold; // need to have usage < secondaryThreshold*maxUsage to go from primary to secondary
    private List<String> primary, secondary;
    private Map<String, Usage> colUsage;
    private Map<String, Boolean> colPrimary; // true if primary

    public TableUsage(int period, double freshness, double primaryThreshold, double secondaryThreshold, List<String> primary, List<String> secondary, Map<String, Usage> colUsage) {

        this.period = period;
        this.freshness = freshness;
        this.primaryThreshold = primaryThreshold;
        this.secondaryThreshold = secondaryThreshold;
        this.primary = primary;
        this.secondary = secondary;
        this.colUsage = colUsage;
        queries = 0;
        maxUsage = 0;
        colPrimary = new HashMap<>();
        for (String col : primary) {
            colPrimary.put(col, true);
        }
        for (String col : secondary) {
            colPrimary.put(col, false);
        }
    }

    public boolean isPrimary(String col) {

        return colPrimary.get(col);
    }

    // If this query triggers an update, the list contains the columns that need to be moved
    public boolean used(List<String> columns) {

        for (String col : columns) {
            Usage usage = colUsage.get(col);
            usage.increment();
        }
        queries++;
        // Check if need to update column locations
        if (queries == period) {
            queries = 0;
            maxUsage = 0;
            for (Usage usage : colUsage.values()) {
                usage.update(freshness);
                if (usage.getUsage() > maxUsage) {
                    maxUsage = usage.getUsage();
                }
            }

            boolean changed = false;
            List<String> newPrimary = new ArrayList<>();
            List<String> newSecondary = new ArrayList<>();
            for (String col : primary) {
                if (colUsage.get(col).getUsage() < secondaryThreshold*maxUsage) {
                    changed = true;
                    newSecondary.add(col);
                } else {
                    newPrimary.add(col);
                }
            }
            for (String col : secondary) {
                if (colUsage.get(col).getUsage() > primaryThreshold*maxUsage) {
                    changed = true;
                    newPrimary.add(col);
                } else {
                    newSecondary.add(col);
                }
            }
            primary = newPrimary;
            secondary = newSecondary;
            // TODO: Actually change the files
            return changed;
        }
        return false;
    }

    public List<String> getPrimary() {
        return primary;
    }

    public List<String> getSecondary() {
        return secondary;
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(period);
        sb.append("\n");
        sb.append(freshness);
        sb.append("\n");
        sb.append(primaryThreshold);
        sb.append("\n");
        sb.append(secondaryThreshold);
        sb.append("\n");
        sb.append(CSV.join(primary, ","));
        sb.append("\n");
        List<String> primaryUsage = new ArrayList<>();
        for (String col : primary) {
            String usage = String.format("%f", colUsage.get(col).getUsage());
            primaryUsage.add(usage);
        }
        sb.append(CSV.join(primaryUsage, ","));
        sb.append("\n");
        sb.append(CSV.join(secondary, ","));
        sb.append("\n");
        List<String> secondaryUsage = new ArrayList<>();
        for (String col : secondary) {
            String usage = String.format("%f", colUsage.get(col).getUsage());
            secondaryUsage.add(usage);
        }
        sb.append(CSV.join(secondaryUsage, ","));
        sb.append("\n");
        return sb.toString();
    }
}
