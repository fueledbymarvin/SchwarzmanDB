import java.io.File;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.List;

/**
 * Created by Frank on 4/12/16.
 */
public class Record {

    int id;
    String values;

    public Record(int id, String values) {

        this.id = id;
        this.values = values;
    }

    public Record getRecord(Table table, int id, List<String> columns) {

        Set<String> needsPrimary = new LinkedHashSet<>();
        Set<String> needsSecondary = new LinkedHashSet<>();
        for (String col : columns) {
            if (table.isPrimary(col)) {
                needsPrimary.add(col);
            } else if (table.isSecondary(col)) {
                needsSecondary.add(col);
            }
            // Should we throw an error if it's an invalid col?
        }
        return new Record(id, columns.get(0));
    }
}
