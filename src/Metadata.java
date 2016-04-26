import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by marvin on 4/7/16.
 */
public class Metadata {

    private static String METADATA_FILENAME = "metadata";
    private static int DEFAULT_PERIOD = 100;
    private static double DEFAULT_FRESHNESS = 0.25;
    private static double DEFAULT_THRESHOLD = 0.2;

    private Path dataPath;
    private Map<String, Table> tables;
    private File metadata;
    private int period; // number of queries to process before update
    private double freshness; // proportion of overall usage that the latest measurement counts for
    private double threshold; // need this proportion of queries to use that set of columns to create a projection

    // Load database
    public static Metadata load(String dataDir) throws IOException {

        File metadata = Paths.get(dataDir, METADATA_FILENAME).toFile();
        try (BufferedReader in = new BufferedReader(new FileReader(metadata))) {
            String line;
            line = forceReadLine(in);
            int period = Integer.parseInt(line);
            line = forceReadLine(in);
            double freshness = Double.parseDouble(line);
            line = forceReadLine(in);
            double threshold = Double.parseDouble(line);
            Metadata res = new Metadata(Paths.get(dataDir), metadata, period, freshness, threshold);

            // Read table infos
            while ((line = in.readLine()) != null) {
                res.tables.put(line, Table.load(res.dataPath, line));
            }
            return res;
        }
    }

    public static String forceReadLine(BufferedReader in) throws IOException {

        String line = in.readLine();
        if (line == null) {
            throw new IllegalArgumentException("Metadata not formatted properly");
        }
        return line;
    }

    private Metadata(Path dataPath, File metadata, int period, double freshness, double threshold) throws IOException {

        this.dataPath = dataPath;
        this.metadata = metadata;
        this.period = period;
        this.freshness = freshness;
        this.threshold = threshold;
        tables = new HashMap<>();
    }

    // New database
    public Metadata(String dir, String name) throws IOException {

        this(dir, name, DEFAULT_PERIOD, DEFAULT_FRESHNESS, DEFAULT_THRESHOLD);
    }

    // New database
    public Metadata(String dir, String name, int period, double freshness, double threshold) throws IOException {

        this(Paths.get(dir, name), Paths.get(dir, name, METADATA_FILENAME).toFile(), period, freshness, threshold);
        Files.createDirectory(dataPath);
        try (Writer out = new FileWriter(metadata, true)) {
            out.write(String.format("%d\n", period));
            out.write(String.format("%f\n", freshness));
            out.write(String.format("%f\n", threshold));
        }
    }

    public Table get(String table) {

        return tables.get(table);
    }

    public void createTable(String name, List<String> columns) throws IOException {

        createTable(name, columns, period, freshness, threshold, true);
    }

    public void createTable(String name, List<String> columns, boolean projectionsEnabled) throws IOException {

        createTable(name, columns, period, freshness, threshold, projectionsEnabled);
    }
    
    public void createTable(String name, List<String> columns, int tablePeriod, double tableFreshness, double tableThreshold, boolean projectionsEnabled) throws IOException {

        Table table = new Table(dataPath, name, columns, tablePeriod, tableFreshness, tableThreshold, projectionsEnabled);
        table.dump();
        tables.put(name, table);
        try (Writer out = new BufferedWriter(new FileWriter(metadata, true))) {
            out.write(name+"\n");
        }
    }
}
