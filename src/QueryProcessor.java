import java.io.*;
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

        table.readLock().lock();
        try {
            List<Record> records = new ArrayList<>();
//            List<String> primaryColsToFetch = new ArrayList<>();
//            List<String> secondaryColsToFetch = new ArrayList<>();
//
//            for (String column : columns) {
//                if (table.isPrimary(column)) {
//                    primaryColsToFetch.add(column);
//                } else if (table.isSecondary(column)) {
//                    secondaryColsToFetch.add(column);
//                }
//            }
//
//            Map<Integer, Map<String, String>> values = new HashMap<>();
//
//            if (primaryColsToFetch.size() > 0) {
//                values = scanFile(Boolean.TRUE, table, primaryColsToFetch, values);
//            }
//
//            if (secondaryColsToFetch.size() > 0) {
//                values = scanFile(Boolean.FALSE, table, secondaryColsToFetch, values);
//            }
//
//            // Convert saved values into records
//            for (Map.Entry<Integer, Map<String, String>> entry : values.entrySet()) {
//                records.add(new Record(table, entry.getKey(), entry.getValue()));
//            }
//
//            // Update table usage
//            if (table.used(columns)) {
//                updateTable(table);
//            }

            return records;
        } finally {
            table.readLock().unlock();
        }
    }

    private Map<Integer, Map<String, String>> scanFile(Boolean isPrimary, Table table, List<String> columns, Map<Integer, Map<String, String>> values) throws IOException {

        // Open appropriate file and save its column names
//        File file;
//        List<String> tableColumns;
//        if (isPrimary) {
//            file = table.getPrimary();
//            tableColumns = table.getPrimaryColumns();
//        } else {
//            file = table.getSecondary();
//            tableColumns = table.getSecondaryColumns();
//        }
//
//        // Iterate through file and save columns in the values map
//        List<String> splitLine;
//        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
//            for (String line = br.readLine(); line != null; line = br.readLine()) {
//                splitLine = CSV.split(line, ",");
//                for (String column : columns) {
//                    Map<String, String> columnValuePairs;
//                    int id = Integer.parseInt(splitLine.get(0));
//                    String newValue = splitLine.get(tableColumns.indexOf(column) + 1);
//
//                    if ((columnValuePairs = values.get(id)) == null) {
//                        columnValuePairs = new HashMap<>();
//                    }
//                    columnValuePairs.put(column, newValue);
//                    values.put(id, columnValuePairs);
//                }
//            }
//        }

        return values;
    }

    public Record read(Table table, int id, List<String> columns) throws IOException {

        table.readLock().lock();
        try {
            Map<String, String> values = new HashMap<>();
//            List<String> primaryColsToFetch = new ArrayList<>();
//            List<String> secondaryColsToFetch = new ArrayList<>();
//
//            for (String column : columns) {
//                if (table.isPrimary(column)) {
//                    primaryColsToFetch.add(column);
//                } else if (table.isSecondary(column)) {
//                    secondaryColsToFetch.add(column);
//                }
//            }
//
//            if (primaryColsToFetch.size() > 0) {
//                values = searchFileForRecord(Boolean.TRUE, table, id, primaryColsToFetch, values);
//            }
//
//            if (secondaryColsToFetch.size() > 0) {
//                values = searchFileForRecord(Boolean.FALSE, table, id, secondaryColsToFetch, values);
//            }
//
//            // Update table usage
//            if (table.used(columns)) {
//                updateTable(table);
//            }

            return new Record(table, id, values);
        } finally {
            table.readLock().unlock();
        }
    }

    private Map<String, String> searchFileForRecord(Boolean isPrimary, Table table, int id, List<String> columns, Map<String, String> values) throws IOException {

        // Open appropriate file and save its column names
//        File file;
//        List<String> tableColumns;
//        if (isPrimary) {
//            file = table.getPrimary();
//            tableColumns = table.getPrimaryColumns();
//        } else {
//            file = table.getSecondary();
//            tableColumns = table.getSecondaryColumns();
//        }
//
//        // Iterate through file and save columns in the values map
//        List<String> splitLine;
//        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
//            for (String line = br.readLine(); line != null; line = br.readLine()) {
//                splitLine = CSV.split(line, ",");
//                for (String column : columns) {
//                    int currentId = Integer.parseInt(splitLine.get(0));
//                    if (currentId == id) {
//                        String newValue = splitLine.get(tableColumns.indexOf(column) + 1);
//                        values.put(column, newValue);
//                    }
//                }
//            }
//        }

        return values;
    }

    public boolean write(Record record) throws IOException {

        Table table = record.getTable();
        if (!table.writeLock().tryLock()) {
            return false;
        }
        try {
//            Map<String, String> vals = record.getValues();
//            int id = table.getNextId();
//            table.incrementNextId();
//            try (
//                    Writer pOut = new BufferedWriter(new FileWriter(table.getPrimary(), true));
//                    Writer sOut = new BufferedWriter(new FileWriter(table.getSecondary(), true))
//            ) {
//                pOut.write(createRow(id, table.getPrimaryColumns(), vals)+"\n");
//                sOut.write(createRow(id, table.getSecondaryColumns(), vals)+"\n");
//            }
            table.dump();
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

    private void updateTable(Table table) {

        synchronized (updateQueue) {
            updateQueue.add(table);
            updateQueue.notifyAll();
        }
    }
}
