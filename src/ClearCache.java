/* 
	use the ClearCache.clear() method to run the bash script to flush the file system cache
*/
import java.lang.Process;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ClearCache {

	public static void main(String[] args) throws IOException {

		clear(args[0]);
		long start = System.nanoTime();
		Path got = Paths.get("/home/marvin/Downloads/got.mp4");
		Files.readAllBytes(got);
		System.out.println(System.nanoTime() - start);
	}

	public static void clear(String pass) {
		try {
			String[] cmd = {"/bin/zsh","-c","echo "+pass+" | sudo -S src/clearCache.sh"};
			Process pb = Runtime.getRuntime().exec(cmd);

			String line;
			BufferedReader input = new BufferedReader(new InputStreamReader(pb.getInputStream()));
			while ((line = input.readLine()) != null) {
				System.out.println(line);
			}
			input.close();
			BufferedReader err = new BufferedReader(new InputStreamReader(pb.getErrorStream()));
			while ((line = err.readLine()) != null) {
				System.out.println(line);
			}
			err.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}