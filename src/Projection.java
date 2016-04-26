import java.io.File;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by marvin on 4/24/16.
 */
public class Projection {

    private Set<String> cols;
    private File file;
    private int useCount;
    private double usage;

    public Projection(String key, double usage) {

        this.usage = usage;
        useCount = 0;
        cols = new TreeSet<>(); // columns stays sorted
        cols.addAll(CSV.split(key, ","));
    }

    public Projection(String key, double usage, File file) {

        this(key, usage);
        this.file = file;
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
        sb.append(usage);
        if (file != null) {
            sb.append(",");
            sb.append(file.getName());
        }
        sb.append("\n");
        return sb.toString();
    }
}
