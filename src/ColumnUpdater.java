import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Created by marvin on 4/19/16.
 */
public class ColumnUpdater extends Thread {

    private Queue<Table> updateQueue;
    private QueryProcessor queryProcessor;

    public ColumnUpdater(Queue<Table> updateQueue, QueryProcessor queryProcessor) {

        this.updateQueue = updateQueue;
        this.queryProcessor = queryProcessor;
    }

    @Override
    public void run() {

        while (true) {

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
                List<String> cols = new ArrayList<>();
                cols.addAll(table.getPrimaryColumns());
                cols.addAll(table.getSecondaryColumns());
                List<Record> records = queryProcessor.scan(table, cols);

                // Write to temporary file
                File newPrimary = File.createTempFile(table.getPrimary().getName(), null);
                File newSecondary = File.createTempFile(table.getSecondary().getName(), null);
                List<String> newPrimaryCols = table.getNewPrimaryColumns();
                List<String> newSecondaryCols = table.getNewSecondaryColumns();
                try (
                        Writer pOut = new BufferedWriter(new FileWriter(newPrimary, true));
                        Writer sOut = new BufferedWriter(new FileWriter(newSecondary, true))
                ) {
                    for (Record r : records) {
                        pOut.write(queryProcessor.createRow(r.getId(), newPrimaryCols, r.getValues())+"\n");
                        sOut.write(queryProcessor.createRow(r.getId(), newSecondaryCols, r.getValues())+"\n");
                    }
                }

                // Switch the files and the primary/secondary column values
                table.writeLock().lock();
                try {
                    newPrimary.renameTo(table.getPrimary());
                    newSecondary.renameTo(table.getSecondary());
                    table.switchToNew();
                } finally {
                    table.writeLock().unlock();
                }
            } catch (IOException e) {
                System.err.println("Could not update: " + e.toString());
            } finally {
                table.readLock().unlock();
            }
        }
    }
}
