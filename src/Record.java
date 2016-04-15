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
    Table table;

    public Record(int id, String values, Table table) {

        this.id = id;
        this.values = values;
        this.table = table;
    }
}
