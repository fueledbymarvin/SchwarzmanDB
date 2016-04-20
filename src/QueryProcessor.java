import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
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
        Record record = new Record(table, id, values);
        return record;
    }


    public void write(Record record) throws IOException {

        Table table = record.getTable();
        Map<String, String> vals = record.getValues();
        writeRow(table.getPrimaryColumns(), vals, table.getPrimary());
        writeRow(table.getSecondaryColumns(), vals, table.getSecondary());
    }

    public void writeRow(List<String> cols, Map<String, String> vals, File file) throws IOException {

        List<String> relevantVals = new ArrayList<>(cols.size());
        for (String col : cols) {
            relevantVals.add(vals.get(col));
        }
        try (Writer out = new FileWriter(file, true)) {
            out.write(CSV.join(relevantVals, ","));
        }
    }
}
