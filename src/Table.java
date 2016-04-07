import java.io.File;
import java.util.Set;

/**
 * Created by marvin on 4/7/16.
 */
public class Table {

    Set<String> primaryCols;
    Set<String> secondaryCols;
    File primary, secondary;

    public Table(Set<String> primaryCols, Set<String> secondaryCols, String primary, String secondary) {

        this.primaryCols = primaryCols;
        this.secondaryCols = secondaryCols;
        this.primary = new File(primary);
        this.secondary = new File(secondary);
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
}
