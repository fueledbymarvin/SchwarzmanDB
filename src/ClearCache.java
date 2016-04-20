/* 
	use the ClearCache.clear() method to run the bash script to flush the file system cache
*/
import java.lang.Process;
import java.io.*;

public class ClearCache {
	public static void clear() {
		String[] env = {"PATH=/bin:/usr/bin"};
		String command = "src/clearCache.sh"; //add directory to this
		try {
			Process process = Runtime.getRuntime().exec(command, env);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}