/**
 * Created by marvin on 4/25/16.
 */
public class Timer {

    private long start;

    public Timer() {
        start = System.nanoTime();
    }

    public long getNano() {
        return System.nanoTime() - start;
    }

    public double getMillis() {
        return getNano() / 1000000.0;
    }

    public double getSeconds() {
        return getMillis() / 1000;
    }
}
