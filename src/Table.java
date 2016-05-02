import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by marvin on 4/14/16.
 */
public class Table {

    private Path dataPath;
    private File tableInfo;
    private String name;
    private int nextId;
    private Config config;
    private Map<String, Projection> projections; // set of all projections sorted by size ascending
    private List<String> columns;
    private ReadWriteLock rwLock;
    private boolean projectionsEnabled, updating;
    private List<Record> updated;

    private static Map<String, Projection> newSortedProjMap() {

        return new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                int size1 = CSV.split(s1, ",").size();
                int size2 = CSV.split(s2, ",").size();
                return Integer.compare(size1, size2);
            }
        });
    }

    public static Table load(Path dataPath, String name) throws IOException {

        File file = Paths.get(dataPath.toString(), name).toFile();
        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            String line;
            line = forceReadLine(in);
            boolean projectionsEnabled = Boolean.parseBoolean(line);
            line = forceReadLine(in);
            int nextId = Integer.parseInt(line);
            line = forceReadLine(in);
            Config config = new Config(line);
            line = forceReadLine(in);
            List<String> columns = CSV.split(line, ",");

            // read projections
            Map<String, Projection> projections = newSortedProjMap();
            while ((line = in.readLine()) != null) {
                String key = line;
                line = forceReadLine(in);
                List<String> projInfo = CSV.split(line, ",");
                Projection proj;
                if (projInfo.size() == 2) {
                    proj = new Projection(key, Double.parseDouble(projInfo.get(0)), Double.parseDouble(projInfo.get(1)), config);
                } else if (projInfo.size() == 3) {
                    File projFile = Paths.get(dataPath.toString(), projInfo.get(2)).toFile();
                    proj = new Projection(key, Double.parseDouble(projInfo.get(0)), Double.parseDouble(projInfo.get(1)), config, projFile);
                } else {
                    throw new IllegalArgumentException("Projection info not formatted properly");
                }
                projections.put(key, proj);
            }

            return new Table(dataPath, name, columns, nextId, config, projections, projectionsEnabled);
        }
    }

    public static String forceReadLine(BufferedReader in) throws IOException {

        String line = in.readLine();
        if (line == null) {
            throw new IllegalArgumentException("Table info not formatted properly");
        }
        return line;
    }

    // brand new table
    public Table(Path dataPath, String name, List<String> columns, Config config, boolean projectionsEnabled) throws IOException {

        this(dataPath, name, columns, 1, config, newSortedProjMap(), projectionsEnabled);
        String key = getKey(columns);
        Projection proj = new Projection(key, 0, 0, config);
        projections.put(key, proj);
        proj.setFile(createProjectionFile());
    }

    private Table(Path dataPath, String name, List<String> columns, int nextId, Config config, Map<String, Projection> projections, boolean projectionsEnabled) {

        this.dataPath = dataPath;
        this.name = name;
        this.columns = columns;
        this.nextId = nextId;
        this.config = config;
        this.projections = projections;
        this.projectionsEnabled = projectionsEnabled;
        tableInfo = Paths.get(dataPath.toString(), name).toFile();
        rwLock = new ReentrantReadWriteLock();
        updating = false;
        updated = new ArrayList<>();
    }

    public File createProjectionFile() throws IOException {

        return Files.createTempFile(dataPath, name, ".proj").toFile();
    }

    public Projection projectionToRead(List<String> cols) {

        for (Projection proj : projections.values()) {
            // Ordered by number of columns
            // Returns smallest projection that contains all relevant columns
            if (proj.hasFile() && proj.getColumns().containsAll(cols)) {
                return proj;
            }
        }
        throw new IllegalArgumentException("Nonexistent columns");
    }

    public List<Projection> projectionsToWrite(List<String> cols) {

        // Returns all projections it would write even if the projection doesn't exist
        // This allows tracking of what would have had to be written if it did exist
        List<Projection> res = new ArrayList<>();
        for (Projection proj : projections.values()) {
            Set<String> projCols = proj.getColumns();
            for (String col : cols) {
                if (projCols.contains(col)) {
                    res.add(proj);
                    break;
                }
            }
        }
        return res;
    }

    public Update wanted(List<String> cols) {

        if (!projectionsEnabled) {
            return null;
        }

        // Add set of columns to possible projections
        String key = getKey(cols);
        if (!projections.containsKey(key)) {
            projections.put(key, new Projection(key, 0, 0, config));
        }
        Projection proj = projections.get(key);

        // Check if should create a new projection for this set of columns
        Update.Action action = proj.read(columns.size(), nextId - 1, colDiff(cols));
        if (action != null) {
            return new Update(action, this, proj);
        }
        return null;
    }

    public Update wrote(Projection proj) {

        if (!projectionsEnabled) {
            return null;
        }

        List<String> cols = new ArrayList<>(proj.getColumns());
        Update.Action action = proj.wrote(columns.size(), nextId - 1, colDiff(cols));
        if (action != null) {
            return new Update(action, this, proj);
        }
        return null;
    }

    public int getNextId() {
        return nextId;
    }

    public void incrementNextId() {
        nextId++;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Lock readLock() {
        return rwLock.readLock();
    }

    public Lock writeLock() {
        return rwLock.writeLock();
    }

    public boolean isUpdating() {
        return updating;
    }

    public void setUpdating(boolean updating) {
        this.updating = updating;
    }

    public List<Record> getUpdated() {
        return updated;
    }

    public void addUpdated(Record newRecord) {
        updated.add(newRecord);
    }

    public void resetUpdated() {
        updated = new ArrayList<>();
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(projectionsEnabled);
        sb.append("\n");
        sb.append(nextId);
        sb.append("\n");
        sb.append(config);
        sb.append("\n");
        sb.append(getKey(columns));
        sb.append("\n");
        for (Projection proj : projections.values()) {
            sb.append(proj.toString());
        }
        return sb.toString();
    }

    // Call this whenever update table's metadata
    public void dump() throws IOException {

        try (Writer out = new BufferedWriter(new FileWriter(tableInfo, false))) {
            out.write(toString());
        }
    }

    private String getKey(List<String> columns) {

        // Get unique key
        Collections.sort(columns);
        return CSV.join(columns, ",");
    }

    private int colDiff(List<String> cols) {

        return projectionToRead(cols).getColumns().size() - cols.size();
    }
}
