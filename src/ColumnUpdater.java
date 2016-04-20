import java.util.Queue;

/**
 * Created by marvin on 4/19/16.
 */
public class ColumnUpdater extends Thread {

    private Queue<Table> updateQueue;

    public ColumnUpdater(Queue<Table> updateQueue) {

        this.updateQueue = updateQueue;
    }

    @Override
    public void run() {

        while (true) {

            Table toUpdate;
            synchronized (updateQueue) {
                // Wait if the queue is empty
                while (updateQueue.isEmpty()) {
                    try {
                        updateQueue.wait();
                    } catch (InterruptedException e) {
                        System.err.println("Interrupted: " + e.toString());
                    }
                }
                toUpdate = updateQueue.remove();
            }
            // TODO: actually update
        }
    }
}
