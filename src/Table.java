import java.io.File;
import java.util.Set;

/**
 * Created by marvin on 4/7/16.
 */
public class Table {

    String name;
    Set<String> primaryCols;
    Set<String> secondaryCols;
    File primary, secondary;

    public Table(String name, Set<String> primaryCols, Set<String> secondaryCols, String primary, String secondary) {

        this.name = name;
        this.primaryCols = primaryCols;
        this.secondaryCols = secondaryCols;
        this.primary = new File(primary);
        this.secondary = new File(secondary);
    }

    public boolean isPrimary(String col) {

        return primaryCols.contains(col);
    }

    public boolean isSecondary(String col) {

        return secondaryCols.contains(col);
    }

    public Set<String> getPrimaryCols() {
        return primaryCols;
    }

    public Set<String> getSecondaryCols() {
        return secondaryCols;
    }

    public File getPrimary() {
        return primary;
    }

    public File getSecondary() {
        return secondary;
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("\n");
        if (!primaryCols.isEmpty()) {
            for (String col : primaryCols) {
                sb.append(col);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("\n");
        if (!secondaryCols.isEmpty()) {
            for (String col : secondaryCols) {
                sb.append(col);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("\n");
        return sb.toString();
    }
}
