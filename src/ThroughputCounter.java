import java.util.List;
import java.util.ArrayList;

/**
 * Created by frankjwu on 4/25/16.
 */
public class ThroughputCounter extends Thread {

    private List<Integer> throughput;
    private Boolean running;
    private int current;

    public ThroughputCounter() {

        this.throughput = new ArrayList<>();
        this.running = false;
        this.current = 0;
    }

    public void run() {
        running = true;

        while (running) {
            try {
                sleep(200);
            } catch (InterruptedException e) {}
            throughput.add(current);
            current = 0;
        }
    }

    public void increment() {

        current++;
    }

    public List<Integer> stopAndReturnThroughput() {

        running = false;
        return throughput;
    }
}
