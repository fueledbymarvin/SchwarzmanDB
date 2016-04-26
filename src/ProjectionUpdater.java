import java.io.*;
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
            Table table;
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
                table = updateQueue.remove();
            }

            table.readLock().lock();
            try {

                // Read all records
                List<String> cols = table.getColumns();
                List<Record> records = queryProcessor.scan(table, cols);

                Queue<List<String>> toCreate = table.getToCreate();
                List<String> projCols;
                while ((projCols = toCreate.poll()) != null) {
                    Timer timer = new Timer();

                    // Write projection
                    Projection proj = table.createProjectionFile(projCols);
                    try (
                            Writer out = new BufferedWriter(new FileWriter(proj.getFile(), true))
                    ) {
                        for (Record r : records) {
                            out.write(queryProcessor.createRow(r.getId(), projCols, r.getValues()) + "\n");
                        }
                    }

                    System.out.println("ProjectionUpdater time: " + timer.getNano());
                }
            } catch (IOException e) {
                System.err.println("Could not update: " + e.toString());
            } finally {
                table.readLock().unlock();
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
