import java.io.File;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * Created by frankjwu on 4/19/16.
 */
public class QueryProcessor {

    private Queue<Table> updateQueue;

    public QueryProcessor(Queue<Table> updateQueue) {

        this.updateQueue = updateQueue;
    }

    public List<Record> scan(Table table, List<String> columns) throws IOException {

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

        // Convert saved values into records
        for (Map.Entry<Integer, Map<String, String>> entry : values.entrySet()) {
            records.add(new Record(table, entry.getKey(), entry.getValue()));
        }

        // Update table usage
        if (table.used(columns)) {
            updateTable(table);
        }

        return records;
    }

    public static Map<Integer, Map<String, String>> scanFile(Boolean isPrimary, Table table, List<String> columns, Map<Integer, Map<String, String>> values) throws IOException {

        // Open appropriate file and save its column names
        File file;
        List<String> tableColumns;
        if (isPrimary) {
            file = table.getPrimary();
            tableColumns = table.getPrimaryColumns();
        } else {
            file = table.getSecondary();
            tableColumns = table.getSecondaryColumns();
        }

        // Iterate through file and save columns in the values map
        List<String> splitLine;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                splitLine = CSV.split(line, ",");
                for (String column : columns) {
                    Map<String, String> columnValuePairs;
                    int id = Integer.parseInt(splitLine.get(0));
                    String newValue = splitLine.get(tableColumns.indexOf(column) + 1);

                    if ((columnValuePairs = values.get(id)) == null) {
                        columnValuePairs = new HashMap<>();
                    }
                    columnValuePairs.put(column, newValue);
                    values.put(id, columnValuePairs);
                }
            }
        }

        return values;
    }

    public Record read(Table table, int id, List<String> columns) {

        Map<String, String> values = new HashMap<>();
        Record record = new Record(table, id, values);
        return record;
    }

    public void write(Record record) throws IOException {

        Table table = record.getTable();
        Map<String, String> vals = record.getValues();
        int id = table.getNextId();
        table.incrementNextId();
        writeRow(id, table.getPrimaryColumns(), vals, table.getPrimary());
        writeRow(id, table.getSecondaryColumns(), vals, table.getSecondary());
    }

    private void writeRow(int id, List<String> cols, Map<String, String> vals, File file) throws IOException {

        List<String> relevantVals = new ArrayList<>(cols.size());
        relevantVals.add(String.valueOf(id));
        for (String col : cols) {
            relevantVals.add(vals.get(col));
        }
        try (Writer out = new FileWriter(file, true)) {
            out.write(CSV.join(relevantVals, ",")+"\n");
        }
    }

    private void updateTable(Table table) {

        synchronized (updateQueue) {
            updateQueue.add(table);
            updateQueue.notifyAll();
        }
    }
}
