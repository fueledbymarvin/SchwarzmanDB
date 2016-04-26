import java.util.List;

/**
 * Created by frankjwu on 4/25/16.
 */
public class TransactionTestResult {

    private List<Integer> throughput;
    private long time;

    public TransactionTestResult(List<Integer> throughput, long time) {

        this.throughput = throughput;
        this.time = time;
    }

    public TransactionTestResult() {
        return;
    }

    public List<Integer> getThroughput() {
        return throughput;
    }

    public long getTime() {
        return time;
    }
}
