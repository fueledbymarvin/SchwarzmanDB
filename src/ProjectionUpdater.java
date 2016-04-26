import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Created by marvin on 4/19/16.
 */
public class ProjectionUpdater extends Thread {

    private Queue<Update> updateQueue;
    private QueryProcessor queryProcessor;
    private boolean shutdown;

    public ProjectionUpdater(Queue<Update> updateQueue, QueryProcessor queryProcessor) {

        this.updateQueue = updateQueue;
        this.queryProcessor = queryProcessor;
        this.shutdown = false;
    }

    @Override
    public void run() {

        while (true) {
            Update update;
            synchronized (updateQueue) {
                // Wait if the queue is empty
                while (updateQueue.isEmpty()) {
                    // Check for shutdown
                    synchronized (this) {
                        if (shutdown) {
                            return;
                        }
                    }

                    try {
                        updateQueue.wait();
                    } catch (InterruptedException e) {
                        System.err.println("Interrupted: " + e.toString());
                    }
                }
                update = updateQueue.remove();
            }

            Table table = update.getTable();
            Projection proj = update.getProjection();
            switch (update.getAction()) {
                case CREATE:
                    table.readLock().lock();
                    try {
                        // Read all records
                        List<String> cols = table.getColumns();
                        List<Record> records = queryProcessor.scan(table, cols);

                        // Write new projection
                        File file = table.createProjectionFile();
                        List<String> projCols = new ArrayList<>(proj.getColumns());
                        try (
                                Writer out = new BufferedWriter(new FileWriter(file, true))
                        ) {
                            for (Record r : records) {
                                out.write(queryProcessor.createRow(r.getId(), projCols, r.getValues()) + "\n");
                            }
                        }
                        proj.setFile(file);
                    } catch (IOException e) {
                        System.err.println("Could not update: " + e.toString());
                    } finally {
                        table.readLock().unlock();
                    }
                    break;
                case DESTROY:
                    table.writeLock().lock();
                    try {
                        // Delete projection file
                        if (!proj.getFile().delete()) {
                            System.err.println("Could not delete file: " + proj.getFile().getName());
                        }
                        proj.setFile(null);
                    } finally {
                        table.writeLock().unlock();
                    }
                    break;
            }

            // Update table info
            try {
                table.dump();
            } catch (IOException e) {
                System.err.println("Could not update table info: " + e.toString());
            }
        }
    }

    synchronized public void shutdown() {

        shutdown = true;
        synchronized (updateQueue) {
            updateQueue.notifyAll();
        }
    }
}
