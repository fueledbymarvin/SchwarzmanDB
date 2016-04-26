/**
 * Created by marvin on 4/25/16.
 */
public class Debug {

    public static boolean _DEBUG = true;

    public static void DEBUG(String msg) {
        
        if (_DEBUG) {
            System.out.println(msg);
        }
    }
}
