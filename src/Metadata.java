import java.io.*;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by marvin on 4/7/16.
 */
public class Metadata {

    private static String METADATA_FILENAME = "metadata";
    private static String PRIMARY_SUFFIX = "_primary";
    private static String SECONDARY_SUFFIX = "_secondary";

    private Map<String, Table> tables;
    private File metadata;

    public Metadata(String dataPath) throws IOException {

        tables = new HashMap<>();
        metadata = Paths.get(dataPath, METADATA_FILENAME).toFile();
        if (!metadata.exists()) {
            metadata.createNewFile();
        }
        readData();
    }

    public Table get(String table) {

        return tables.get(table);
    }

    public void createTable(String name, List<String> columns) throws IOException {

        Map<String, Usage> colUsage = new HashMap<>();
        for (String col : columns) {
            colUsage.put(col, new Usage(0));
        }
        TableUsage tableUsage = new TableUsage(100, 0.25, 0.6, 0.3, columns, new ArrayList<String>(), colUsage); // move these values into a config
        Table table = new Table(name, name+PRIMARY_SUFFIX, name+SECONDARY_SUFFIX, tableUsage);
        tables.put(name, table);
        Writer out = new FileWriter(metadata, true);
        out.write(table.toString());
        out.close();
    }

    private void readData() throws IOException {
        
        try (BufferedReader in = new BufferedReader(new FileReader(metadata))) {
            String line;
            while ((line = in.readLine()) != null) {
                String name = line;
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                int period = Integer.parseInt(line);
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                double freshness = Double.parseDouble(line);
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                double primaryThreshold = Double.parseDouble(line);
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                double secondaryThreshold = Double.parseDouble(line);

                Map<String, Usage> colUsage = new HashMap<>();
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                List<String> primary = readColumns(line);
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                List<String> primaryUsage = readColumns(line);
                if (primary.size() != primaryUsage.size()) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                for (int i = 0; i < primary.size(); i++) {
                    colUsage.put(primary.get(i), new Usage(Double.parseDouble(primaryUsage.get(i))));
                }

                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                List<String> secondary = readColumns(line);
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                List<String> secondaryUsage = readColumns(line);
                if (secondary.size() != secondaryUsage.size()) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                for (int i = 0; i < secondary.size(); i++) {
                    colUsage.put(secondary.get(i), new Usage(Double.parseDouble(secondaryUsage.get(i))));
                }

                TableUsage tableUsage = new TableUsage(period, freshness, primaryThreshold, secondaryThreshold, primary, secondary, colUsage);
                Table table = new Table(name, name + PRIMARY_SUFFIX, name + SECONDARY_SUFFIX, tableUsage);
                tables.put(name, table);
            }
        }
    }

    private List<String> readColumns(String line) {

        List<String> columns = new ArrayList<>();
        if (!line.isEmpty()) {
            int commaIndex;
            while ((commaIndex = line.indexOf(',')) != -1) {
                columns.add(line.substring(0, commaIndex));
                if (commaIndex + 1 == line.length()) {
                    // trailing comma for some reason
                    return columns;
                }
                line = line.substring(commaIndex + 1);
            }
            columns.add(line);
        }
        return columns;
    }
}
