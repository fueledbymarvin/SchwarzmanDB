import java.util.HashMap;
import java.util.Map;

/**
 * Created by marvin on 4/7/16.
 */
public class Metadata {

    private Map<String, Table> tables;

    public Metadata(String path) {

        tables = new HashMap<>();
    }

    public Table get(String table) {

        return tables.get(table);
    }
}
