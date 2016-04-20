import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.util.jar.Pack200;

/**
 * Created by frankjwu on 4/19/16.
 */
public class QueryProcessor {

    public List<Record> scan(Table table, List<String> columns) {

        List<Record> records = new ArrayList<>();
        List<String> primaryColsToFetch = new ArrayList<>();
        List<String> secondaryColsToFetch = new ArrayList<>();

        for (String column : columns) {
            if (table.isPrimary(column)) {
                primaryColsToFetch.add(column);
            } else if (table.isSecondary(column)) {
                secondaryColsToFetch.add(column);
            }
        }

        Map<Integer, Map<String, String>> values = new HashMap<>();

        if (primaryColsToFetch.size() > 0) {
            values = scanFile(Boolean.TRUE, table, primaryColsToFetch, values);
        }

        if (secondaryColsToFetch.size() > 0) {
            values = scanFile(Boolean.FALSE, table, secondaryColsToFetch, values);
        }

        // TODO: create records

        return records;
    }

    public Map<Integer, Map<String, String>> scanFile(Boolean isPrimary, Table table, List<String> columns, Map<Integer, Map<String, String>> values) {

        File file;
        if (isPrimary) {
            file = table.getPrimary();
        } else {
            file = table.getSecondary();
        }

        // TODO: go through line by line and save columns in value
        for (String column : columns) {
            Map<String, String> columnValuePairs;
            if ((columnValuePairs = values.get(column)) == null) {
                columnValuePairs = new HashMap<>();
            }
//            columnValuePairs.put(column, );
//            values.put(id, columnValuePairs);
        }

        return values;
    }

    public Record read(Table table, int id, List<String> columns) {

        Map<String, String> values = new HashMap<>();
        Record record = new Record(table, id, values, Record.Type.READ);
        return record;
    }

    public void write(Record record) {

        return;
    }
}