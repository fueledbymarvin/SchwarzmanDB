import java.io.File;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by marvin on 4/7/16.
 */
public class Table {

    private String name;
    private File primary, secondary;
    private TableUsage tableUsage;
    private int nextId;
    private ReadWriteLock rwLock;
    private boolean writeable;

    public Table(String name, int nextId, File primary, File secondary, TableUsage tableUsage) {

        this.name = name;
        this.nextId = nextId;
        this.primary = primary;
        this.secondary = secondary;
        this.tableUsage = tableUsage;
        this.rwLock = new ReentrantReadWriteLock();
        writeable = true;
    }

    public boolean isPrimary(String col) {

        return tableUsage.isPrimary(col);
    }

    public boolean isSecondary(String col) {

        return !isPrimary(col);
    }

    public File getPrimary() {
        return primary;
    }

    public File getSecondary() {
        return secondary;
    }

    public List<String> getPrimaryColumns() {

        return tableUsage.getPrimary();
    }

    public List<String> getSecondaryColumns() {

        return tableUsage.getSecondary();
    }

    public List<String> getNewPrimaryColumns() {

        return tableUsage.getNewPrimary();
    }

    public List<String> getNewSecondaryColumns() {

        return tableUsage.getNewSecondary();
    }

    public void switchToNew() {
        tableUsage.switchToNew();
    }

    public int getNextId() {
        return nextId;
    }

    public void incrementNextId() {
        nextId++;
    }

    public Lock readLock() {
        return rwLock.readLock();
    }

    public Lock writeLock() {
        return rwLock.writeLock();
    }

    public boolean isWriteable() {

        return writeable;
    }

    public void setWriteable(boolean writeable) {

        this.writeable = writeable;
    }

    public boolean used(List<String> columns) {

        // Should do something if this returns true
        return tableUsage.used(columns);
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("\n");
        sb.append(nextId);
        sb.append("\n");
        sb.append(tableUsage.toString());
        return sb.toString();
    }

    public int getNumCols() {
        
        return tableUsage.getPrimary().length + tableUsage.getSecondary().length;
    }
}
