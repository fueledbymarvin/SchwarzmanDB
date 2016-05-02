import java.io.*;
import java.util.*;

/**
 * Created by frankjwu on 4/19/16.
 */
public class QueryProcessor {

    private Queue<Update> updateQueue;

    public QueryProcessor(Queue<Update> updateQueue) {

        this.updateQueue = updateQueue;
    }

    public List<Record> scan(Table table, List<String> columns) throws IOException {

        table.readLock().lock();
        try {
            Projection projection = table.projectionToRead(columns);
            List<Record> records = scanFile(table, projection, columns);
            checkUpdate(table, columns, null);

            return records;
        } finally {
            table.readLock().unlock();
        }
    }

    private List<Record> scanFile(Table table, Projection projection, List<String> columns) throws IOException {

        List<Record> records = new ArrayList<>();

        // Open appropriate file and save its column names
        File file = projection.getFile();
        List<String> projectionColumns = new ArrayList<>(projection.getColumns());

        // Iterate through file and save columns in the values map
        List<String> splitLine;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                Map<String, String> values = new HashMap<>();
                splitLine = CSV.split(line, ",");
                int id = Integer.parseInt(splitLine.get(0));

                for (String column : columns) {
                    String newValue = splitLine.get(projectionColumns.indexOf(column) + 1); // + 1 to skip id
                    values.put(column, newValue);
                }
                records.add(new Record(table, id, values));
            }
        }

        return records;
    }

    public Record read(Table table, int id, List<String> columns) throws IOException {

        table.readLock().lock();
        try {
            Projection projection = table.projectionToRead(columns);
            Record record = findRecord(table, id, projection, columns);
            checkUpdate(table, columns, null);

            return record;
        } finally {
            table.readLock().unlock();
        }
    }

    private Record findRecord(Table table, int id, Projection projection, List<String> columns) throws IOException {

        // Open appropriate file and save its column names
        File file = projection.getFile();
        List<String> projectionColumns = new ArrayList<>(projection.getColumns());

        // Iterate through file and save columns in the values map
        List<String> splitLine;
        int currentId;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                splitLine = CSV.split(line, ",");
                currentId = Integer.parseInt(splitLine.get(0));
                if (currentId == id) {
                    Map<String, String> values = new HashMap<>();
                    for (String column : columns) {
                        String newValue = splitLine.get(projectionColumns.indexOf(column) + 1); // + 1 to skip id
                        values.put(column, newValue);
                    }
                    return new Record(table, id, values);
                }
            }
        }
        return null;
    }

    public boolean write(Record record) throws IOException {

        Table table = record.getTable();
        table.writeLock().lock();
        try {
            Map<String, String> vals = record.getValues();
            List<String> cols = record.getCols();
            record.setId(table.getNextId());
            table.incrementNextId();
            List<Projection> projections = table.projectionsToWrite(cols);
            for (Projection projection : projections) {
                try (
                    Writer pOut = new BufferedWriter(new FileWriter(projection.getFile(), true));
                ) {
                    pOut.write(createRow(record.getId(), new ArrayList<>(projection.getColumns()), vals)+"\n");
                }
            }
            if (table.isUpdating()) {
                table.addUpdated(record);
            }
            table.dump();
            checkUpdate(table, null, projections);
            return true;
        } finally {
            table.writeLock().unlock();
        }
    }

    public String createRow(int id, List<String> cols, Map<String, String> vals) {

        List<String> relevantVals = new ArrayList<>(cols.size());
        relevantVals.add(String.valueOf(id));
        for (String col : cols) {
            relevantVals.add(vals.get(col));
        }
        return CSV.join(relevantVals, ",");
    }

    private void checkUpdate(Table table, List<String> columns, List<Projection> projections) {

        // Check for creating projections
        if (columns != null) {
            addUpdate(table.wanted(columns));
        }
        // Check for deleting columns
        if (projections != null) {
            for (Projection proj : projections) {
                addUpdate(table.wrote(proj));
            }
        }
    }

    private void addUpdate(Update update) {

        if (update != null) {
            synchronized (updateQueue) {
                updateQueue.add(update);
                updateQueue.notifyAll();
            }
        }
    }
}
