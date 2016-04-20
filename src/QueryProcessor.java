import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by frankjwu on 4/19/16.
 */
public class QueryProcessor {

    public List<Record> scan(Table table, List<String> columns) {

        List<Record> records = new ArrayList<>();
        return records;
    }

    public Record read(Table table, int id, List<String> columns) {

        Map<String, String> values = new HashMap<>();
        Record record = new Record(table, id, values, READ);
        return record;
    }


    public void write(Record record) {

        return;
    }
}
