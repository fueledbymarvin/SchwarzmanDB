import java.io.File;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;

/**
 * Created by Frank on 4/12/16.
 */
public class Record {

    int id;
    Map<String, String> values;
    Table table;

    public Record(Table table, int id, Map<String, String> values) {

        this.id = id;
        this.values = values;
        this.table = table;
    }

    public int getId() {
        return id;
    }

    public Map<String, String> getValues() {
        return values;
    }

    public Table getTable() {
        return table;
    }
}
