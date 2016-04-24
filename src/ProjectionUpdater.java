import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Created by marvin on 4/19/16.
 */
public class ProjectionUpdater extends Thread {

    private Queue<Table> updateQueue;
    private QueryProcessor queryProcessor;
    private boolean shutdown;

    public ProjectionUpdater(Queue<Table> updateQueue, QueryProcessor queryProcessor) {

        this.updateQueue = updateQueue;
        this.queryProcessor = queryProcessor;
        this.shutdown = false;
    }

    @Override
    public void run() {

        while (true) {
            // Check for shutdown
            synchronized (this) {
                if (shutdown) {
                    break;
                }
            }

            Table table;
            synchronized (updateQueue) {
                // Wait if the queue is empty
                while (updateQueue.isEmpty()) {
                    try {
                        updateQueue.wait();
                    } catch (InterruptedException e) {
                        System.err.println("Interrupted: " + e.toString());
                    }
                }
                table = updateQueue.remove();
            }

            table.readLock().lock();
            try {
                // Read all records
                List<String> cols = table.getColumns();
                List<Record> records = queryProcessor.scan(table, cols);

                // Write projection
                Projection proj = table.createProjectionFile(cols);
                List<String> projCols = new ArrayList<>(proj.getColumns());
                try (
                        Writer out = new BufferedWriter(new FileWriter(proj.getFile(), true))
                ) {
                    for (Record r : records) {
                        out.write(queryProcessor.createRow(r.getId(), projCols, r.getValues()) + "\n");
                    }
                }
                table.dump();
            } catch (IOException e) {
                System.err.println("Could not update: " + e.toString());
            } finally {
                table.readLock().unlock();
            }
        }
    }

    synchronized public void shutdown() {
        shutdown = true;
    }
}
