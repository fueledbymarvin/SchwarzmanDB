import java.io.File;
import java.util.*;

/**
 * Created by marvin on 4/24/16.
 */
public class Projection {

    private Set<String> cols;
    private File file;
    private int readCount, writeCount;
    private double readFreq, writeFreq;
    private Config config;

    public Projection(String key, double readFreq, double writeFreq, Config config) {

        this.readFreq = readFreq;
        this.writeFreq = writeFreq;
        this.config = config;
        readCount = 0;
        writeCount = 0;
        cols = new TreeSet<>(); // columns stays sorted
        cols.addAll(CSV.split(key, ","));
    }

    public Projection(String key, double readFreq, double writeFreq, Config config, File file) {

        this(key, readFreq, writeFreq, config);
        this.file = file;
    }

    public boolean read() {

        readCount++;
        return readCount + writeCount == config.getPeriod();
    }

    public boolean wrote() {

        writeCount++;
        return readCount + writeCount == config.getPeriod();
    }

    public Update.Action check(int nRecords, int colDiff) {

        int period = config.getPeriod();
        double freshness = config.getFreshness();
        readFreq = readFreq*(1-freshness) + freshness*readCount/period;
        writeFreq = writeFreq*(1-freshness) + freshness*writeCount/period;
        readCount = 0;
        writeCount = 0;

        // TODO BETTER FORMULA
        double writeCost = writeFreq;
        double readBenefit = readFreq * colDiff;
        double ratio;
        if (writeCost == 0) {
            ratio = config.getCreateThreshold();
        } else {
            ratio = readBenefit / writeCost;
        }
        if (!hasFile() && ratio >= config.getCreateThreshold()) {
            return Update.Action.CREATE;
        } else if (hasFile() && ratio <= config.getDestroyThreshold()) {
            return Update.Action.DESTROY;
        } else {
            return null;
        }
    }

    public Set<String> getColumns() {
        return cols;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public boolean hasFile() {
        return file != null;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(CSV.join(new ArrayList<>(cols), ","));
        sb.append("\n");
        sb.append(readFreq);
        sb.append(writeFreq);
        if (file != null) {
            sb.append(",");
            sb.append(file.getName());
        }
        sb.append("\n");
        return sb.toString();
    }
}
