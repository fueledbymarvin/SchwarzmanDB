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
    private int queries; // number of queries since last measurement
    private int period; // number of queries to process before update
    private double freshness; // proportion of overall usage that the latest measurement counts for
    private double threshold;
    private Queue<String> toCreate;
    private Map<String, Projection> projections; // set of all projections sorted by size ascending
    private List<String> columns;
    private ReadWriteLock rwLock;

    private static Map<String, Projection> newSortedProjMap() {

        return new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                int size1 = CSV.split(s1, ",").size();
                int size2 = CSV.split(s1, ",").size();
                return Integer.compare(size1, size2);
            }
        });
    }

    public static Table load(Path dataPath, String name) throws IOException {

        File file = Paths.get(dataPath.toString(), name).toFile();
        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            String line;
            line = forceReadLine(in);
            int nextId = Integer.parseInt(line);
            line = forceReadLine(in);
            int period = Integer.parseInt(line);
            line = forceReadLine(in);
            double freshness = Double.parseDouble(line);
            line = forceReadLine(in);
            double threshold = Double.parseDouble(line);
            line = forceReadLine(in);
            List<String> columns = CSV.split(line, ",");

            // read projections
            Map<String, Projection> projections = newSortedProjMap();
            while ((line = in.readLine()) != null) {
                String key = line;
                line = forceReadLine(in);
                List<String> projInfo = CSV.split(line, ",");
                Projection proj;
                if (projInfo.size() == 1) {
                    proj = new Projection(key, Double.parseDouble(projInfo.get(0)));
                } else if (projInfo.size() == 2) {
                    File projFile = Paths.get(dataPath.toString(), projInfo.get(1)).toFile();
                    proj = new Projection(key, Double.parseDouble(projInfo.get(0)), projFile);
                } else {
                    throw new IllegalArgumentException("Projection info not formatted properly");
                }
                projections.put(key, proj);
            }

            return new Table(dataPath, name, columns, nextId, period, freshness, threshold, projections);
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
    public Table(Path dataPath, String name, List<String> columns, int period, double freshness, double threshold) throws IOException {

        this(dataPath, name, columns, 1, period, freshness, threshold, newSortedProjMap());
        String key = getKey(columns);
        projections.put(key, new Projection(key, 0));
        createProjectionFile(columns);
    }

    private Table(Path dataPath, String name, List<String> columns, int nextId, int period, double freshness, double threshold, Map<String, Projection> projections) {

        this.dataPath = dataPath;
        this.name = name;
        this.columns = columns;
        this.nextId = nextId;
        this.period = period;
        this.freshness = freshness;
        this.threshold = threshold;
        this.projections = projections;
        tableInfo = Paths.get(dataPath.toString(), name).toFile();
        queries = 0;
        toCreate = new LinkedList<>();
        rwLock = new ReentrantReadWriteLock();
    }

    public boolean used(List<String> columns) {

        String key = getKey(columns);
        // Update usage
        if (!projections.containsKey(key)) {
            projections.put(key, new Projection(key, 0));
        }
        Projection proj = projections.get(key);
        proj.increment();

        queries++;
        // Check if need to update column locations
        if (queries == period) {
            queries = 0;
            for (Map.Entry<String, Projection> entry : projections.entrySet()) {
                Projection p = entry.getValue();
                String k = entry.getKey();
                p.update(freshness);
                if (p.getFile() == null && p.getUsage() / period > threshold) {
                    toCreate.add(k);
                }
            }
            return true;
        }
        return false;
    }

    public Projection createProjectionFile(List<String> cols) throws IOException {

        Projection proj = projections.get(getKey(cols));
        File file = Files.createTempFile(dataPath, name, ".proj").toFile();
        proj.setFile(file);
        return proj;
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

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(nextId);
        sb.append("\n");
        sb.append(period);
        sb.append("\n");
        sb.append(freshness);
        sb.append("\n");
        sb.append(threshold);
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
}
