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
        if (!readData()) {
            throw new IllegalArgumentException("Metadata not formatted properly");
        }
    }

    public Table get(String table) {

        return tables.get(table);
    }

    public void createTable(String name, List<String> columns) throws IOException {

        Set<String> primary = new LinkedHashSet<>();
        for (String col : columns) {
            primary.add(col);
        }
        Table table = new Table(name, primary, new LinkedHashSet<String>(), name + PRIMARY_SUFFIX, name + SECONDARY_SUFFIX);
        Writer out = new FileWriter(metadata, true);
        out.write(table.toString());
        out.close();
    }

    private boolean readData() throws IOException {

        BufferedReader in = new BufferedReader(new FileReader(metadata));
        String line;
        while ((line = in.readLine()) != null) {
            String name = line;
            line = in.readLine();
            if (line == null) {
                in.close();
                return false;
            }
            Set<String> primary = readColumns(line);
            line = in.readLine();
            if (line == null) {
                in.close();
                return false;
            }
            Set<String> secondary = readColumns(line);
            Table table = new Table(name, primary, secondary, name + PRIMARY_SUFFIX, name + SECONDARY_SUFFIX);
            tables.put(name, table);
        }
        in.close();
        return true;
    }

    private Set<String> readColumns(String line) {

        Set<String> columns = new LinkedHashSet<>();
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
        return columns;
    }
}
