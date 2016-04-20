import java.io.File;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;

/**
 * Created by Frank on 4/12/16.
 */
public class Record {

    public enum Type {
        READ,
        WRITE
    }
    private Type type;
    private int id;
    private Map<String, String> values;
    private Table table;

    public Record(Table table, int id, Map<String, String> values) {

        this.table = table;
        this.id = id;
        this.values = values;
        type = Type.READ;
    }

    public Record(Table table, Map<String, String> values) {

        this.table = table;
        this.values = values;
        type = Type.WRITE;
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

    public Type getType() {
        return type;
    }
}
