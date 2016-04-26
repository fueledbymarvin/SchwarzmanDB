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

    private Path dataPath;
    private Map<String, Table> tables;
    private File metadata;
    private Config config;

    // Load database
    public static Metadata load(String dataDir) throws IOException {

        File metadata = Paths.get(dataDir, METADATA_FILENAME).toFile();
        try (BufferedReader in = new BufferedReader(new FileReader(metadata))) {
            String line;
            line = forceReadLine(in);
            Config config = new Config(line);
            Metadata res = new Metadata(Paths.get(dataDir), metadata, config);

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

    private Metadata(Path dataPath, File metadata, Config config) throws IOException {

        this.dataPath = dataPath;
        this.metadata = metadata;
        this.config = config;
        tables = new HashMap<>();
    }

    // New database
    public Metadata(String dir, String name) throws IOException {

        this(dir, name, Config.defaultConfig());
    }

    // New database
    public Metadata(String dir, String name, Config config) throws IOException {

        this(Paths.get(dir, name), Paths.get(dir, name, METADATA_FILENAME).toFile(), config);
        Files.createDirectory(dataPath);
        try (Writer out = new FileWriter(metadata, true)) {
            out.write(config.toString() + "\n");
        }
    }

    public Table get(String table) {

        return tables.get(table);
    }

    public void createTable(String name, List<String> columns) throws IOException {

        createTable(name, columns, config, true);
    }

    public void createTable(String name, List<String> columns, boolean projectionsEnabled) throws IOException {

        createTable(name, columns, config, projectionsEnabled);
    }
    
    public void createTable(String name, List<String> columns, Config config, boolean projectionsEnabled) throws IOException {

        Table table = new Table(dataPath, name, columns, config, projectionsEnabled);
        table.dump();
        tables.put(name, table);
        try (Writer out = new BufferedWriter(new FileWriter(metadata, true))) {
            out.write(name+"\n");
        }
    }
}
