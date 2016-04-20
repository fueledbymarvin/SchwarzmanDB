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
    private static int DEFAULT_PERIOD = 100;
    private static double DEFAULT_FRESHNESS = 0.25;
    private static double DEFAULT_PRIMARY_THRESHOLD = 0.75;
    private static double DEFAULT_SECONDARY_THRESHOLD = 0.5;

    private Map<String, Table> tables;
    private File metadata;
    private int period; // number of queries to process before update
    private double freshness; // proportion of overall usage that the latest measurement counts for
    private double primaryThreshold; // need to have usage > primaryThreshold*maxUsage to go from secondary to primary
    private double secondaryThreshold; // need to have usage < secondaryThreshold*maxUsage to go from primary to secondary

    public Metadata(String dataPath) throws IOException {

        period = DEFAULT_PERIOD;
        freshness = DEFAULT_FRESHNESS;
        primaryThreshold = DEFAULT_PRIMARY_THRESHOLD;
        secondaryThreshold = DEFAULT_SECONDARY_THRESHOLD;
        init(dataPath);
    }
    
    public Metadata(String dataPath, int period, double freshness, double primaryThreshold, double secondaryThreshold) throws IOException {
        
        this.period = period;
        this.freshness = freshness;
        this.primaryThreshold = primaryThreshold;
        this.secondaryThreshold = secondaryThreshold;
        init(dataPath);
    }
    
    public void init(String dataPath) throws IOException {

        tables = new HashMap<>();
        metadata = Paths.get(dataPath, METADATA_FILENAME).toFile();
        if (!metadata.exists()) {
            metadata.createNewFile();
            try (Writer out = new FileWriter(metadata, true)) {
                out.write(String.format("%d\n", period));
                out.write(String.format("%f\n", freshness));
                out.write(String.format("%f\n", primaryThreshold));
                out.write(String.format("%f\n", secondaryThreshold));
            }
        }
        readData();
    }

    public Table get(String table) {

        return tables.get(table);
    }

    public void createTable(String name, List<String> columns) throws IOException {

        createTable(name, columns, period, freshness, primaryThreshold, secondaryThreshold);
    }
    
    public void createTable(String name, List<String> columns, int tablePeriod, double tableFreshness,
                            double tablePrimaryThreshold, double tableSecondaryThreshold) throws IOException {

        Map<String, Usage> colUsage = new HashMap<>();
        for (String col : columns) {
            colUsage.put(col, new Usage(0));
        }
        TableUsage tableUsage = new TableUsage(tablePeriod, tableFreshness,
                tablePrimaryThreshold, tableSecondaryThreshold, columns, new ArrayList<String>(), colUsage);
        Table table = new Table(name, 1, name+PRIMARY_SUFFIX, name+SECONDARY_SUFFIX, tableUsage);
        tables.put(name, table);
        try (Writer out = new FileWriter(metadata, true)) {
            out.write(table.toString());
        }
    }

    private void readData() throws IOException {
        
        try (BufferedReader in = new BufferedReader(new FileReader(metadata))) {
            String line;
            // read in default parameters
            line = in.readLine();
            if (line == null) {
                throw new IllegalArgumentException("Metadata not formatted properly");
            }
            period = Integer.parseInt(line);
            line = in.readLine();
            if (line == null) {
                throw new IllegalArgumentException("Metadata not formatted properly");
            }
            freshness = Double.parseDouble(line);
            line = in.readLine();
            if (line == null) {
                throw new IllegalArgumentException("Metadata not formatted properly");
            }
            primaryThreshold = Double.parseDouble(line);
            line = in.readLine();
            if (line == null) {
                throw new IllegalArgumentException("Metadata not formatted properly");
            }
            secondaryThreshold = Double.parseDouble(line);

            while ((line = in.readLine()) != null) {
                String name = line;
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                int nextId = Integer.parseInt(line);
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                int tablePeriod = Integer.parseInt(line);
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                double tableFreshness = Double.parseDouble(line);
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                double tablePrimaryThreshold = Double.parseDouble(line);
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                double tableSecondaryThreshold = Double.parseDouble(line);

                Map<String, Usage> colUsage = new HashMap<>();
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                List<String> primary = CSV.split(line, ",");
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                List<String> primaryUsage = CSV.split(line, ",");
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
                List<String> secondary = CSV.split(line, ",");
                line = in.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                List<String> secondaryUsage = CSV.split(line, ",");
                if (secondary.size() != secondaryUsage.size()) {
                    throw new IllegalArgumentException("Metadata not formatted properly");
                }
                for (int i = 0; i < secondary.size(); i++) {
                    colUsage.put(secondary.get(i), new Usage(Double.parseDouble(secondaryUsage.get(i))));
                }

                TableUsage tableUsage = new TableUsage(tablePeriod, tableFreshness,
                        tablePrimaryThreshold, tableSecondaryThreshold, primary, secondary, colUsage);
                Table table = new Table(name, nextId, name + PRIMARY_SUFFIX, name + SECONDARY_SUFFIX, tableUsage);
                tables.put(name, table);
            }
        }
    }
}
