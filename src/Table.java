import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by marvin on 4/7/16.
 */
public class Table {

    String name;
    List<String> primaryCols;
    List<String> secondaryCols;
    Map<String, Usage> colUsage;
    File primary, secondary;

    public Table(String name, List<String> primaryCols, List<String> secondaryCols, String primary, String secondary, Map<String, Usage> colUsage) {

        this.name = name;
        this.primaryCols = primaryCols;
        this.secondaryCols = secondaryCols;
        this.primary = new File(primary);
        this.secondary = new File(secondary);
        this.colUsage = colUsage;
    }

    public boolean isPrimary(String col) {

        return primaryCols.contains(col);
    }

    public boolean isSecondary(String col) {

        return secondaryCols.contains(col);
    }

    public List<String> getPrimaryCols() {
        return primaryCols;
    }

    public List<String> getSecondaryCols() {
        return secondaryCols;
    }

    public File getPrimary() {
        return primary;
    }

    public File getSecondary() {
        return secondary;
    }

    public void used(List<String> columns) {

        for (String col : columns) {
            colUsage.get(col).increment();
        }
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("\n");
        sb.append(join(primaryCols, ","));
        sb.append("\n");
        List<String> primaryUsage = new ArrayList<>();
        for (String col : primaryCols) {
            String usage = String.format("%f", colUsage.get(col).getUsage());
            primaryUsage.add(usage);
        }
        sb.append(join(primaryUsage, ","));
        sb.append("\n");
        sb.append(join(secondaryCols, ","));
        sb.append("\n");
        List<String> secondaryUsage = new ArrayList<>();
        for (String col : secondaryCols) {
            String usage = String.format("%f", colUsage.get(col).getUsage());
            secondaryUsage.add(usage);
        }
        sb.append(join(secondaryUsage, ","));
        sb.append("\n");
        return sb.toString();
    }

    private String join(List<String> strs, String delim) {

        StringBuilder sb = new StringBuilder();
        if (!strs.isEmpty()) {
            for (String str : strs) {
                sb.append(str);
                sb.append(delim);
            }
            for (int i = 0; i < delim.length(); i++) {
                sb.deleteCharAt(sb.length() - 1 - i);
            }
        }
        return sb.toString();
    }
}
