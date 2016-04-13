import java.io.File;
import java.util.Set;

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

    public getRecord(Table table, int id, List<String> columns) {

        Set<String> needsPrimary;
        Set<String> needsSecondary;
        for (String col : columns) {
            if (isPrimaryCol(col)) {
                needsPrimary.add(col);
            } else if (isSecondaryCol(col)) {
                needsSecondary.add(col);
            }
            // Should we throw an error if it's an invalid col?
        }
    }
}
