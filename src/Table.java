import java.io.File;
import java.util.*;

/**
 * Created by marvin on 4/7/16.
 */
public class Table {

    private String name;
    private File primary, secondary;
    private TableUsage tableUsage;

    public Table(String name, String primary, String secondary, TableUsage tableUsage) {

        this.name = name;
        this.primary = new File(primary);
        this.secondary = new File(secondary);
        this.tableUsage = tableUsage;
    }

    public boolean isPrimary(String col) {

        return tableUsage.isPrimary(col);
    }

    public boolean isSecondary(String col) {

        return !isPrimary(col);
    }

    public File getPrimary() {
        return primary;
    }

    public File getSecondary() {
        return secondary;
    }

    public boolean used(List<String> columns) {

        // Should do something if this returns true
        return tableUsage.used(columns);
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("\n");
        sb.append(tableUsage.toString());
        return sb.toString();
    }
}
