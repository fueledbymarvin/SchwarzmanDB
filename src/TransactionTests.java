import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class TransactionTests {

	final static String SEEDSTRING = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	public static void main(String args[]) throws IOException {

		Queue<Update> updateQueue = new LinkedList<>();
		QueryProcessor qp = new QueryProcessor(updateQueue);
		ProjectionUpdater updater = new ProjectionUpdater(updateQueue, qp);
		updater.start();

		// Test parameters
		int numCols = 10;
		int numRecords = 10000;
		int recordLength = 100;

		// Create and setup new table
//		 deleteDirectory(new File("/Users/frankjwu/Downloads/test"));
//		 Metadata metadata = new Metadata("/Users/frankjwu/Downloads/", "test");
//		deleteDirectory(new File("/home/accts/fjw22/test"));
//		Metadata metadata = new Metadata("/home/accts/fjw22", "test");
		deleteDirectory(new File("/home/marvin/Downloads/test"));
		Metadata metadata = new Metadata("/home/marvin/Downloads/", "test");
		List<String> columns = new ArrayList<>();
		for (int i = 0; i < numCols; i++){
			columns.add("Column " + i);
		}

		Table hybrid = metadata.createTable("Hybrid", columns);
		Table row = metadata.createTable("Row", columns, false);

		// Insert Random Records
		System.out.println("Writing random records...");
		long startTime = System.nanoTime();
		tablePopulator(hybrid, qp, recordLength, numRecords);
		tablePopulator(row, qp, recordLength, numRecords);
		long estimatedTime = System.nanoTime() - startTime;
		System.out.println("Total Elapsed Time was: " + estimatedTime + "\n");

		// Obtain password for permissions to clear cache
		String password;
		if (args.length == 0) {
			password = ""; // no password provided
		} else {
			password = args[0];
		}

		// Test 1 -- Transaction 1 accesses the same column about 90% of the time, with the other 10% split evenly
		// between the other 9 columns. This will result in a new projection that will eventually increase throughput.
		// Transaction 2 accesses every column at an equal probability, so it doesn't have the same benefits of
		// Transaction 1.
		double[] prob = {0.91, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01};
		System.out.println("Running hybrid");
		TransactionTestResult test1 = testFunction(10, 1, numRecords, 5000, prob, hybrid, qp, password);
		System.out.println("The Time was: " + test1.getTime());
		System.out.println("Throughput: " + test1.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(test1.getThroughput().get(i));
		}
		System.out.println("Running row");
		TransactionTestResult test2 = testFunction(10, 1, numRecords, 5000, prob, row, qp, password);
		System.out.println("The Time was: " + test2.getTime());
		System.out.println("Throughput: " + test2.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(test2.getThroughput().get(i));
		}

		// Test 2 -- Real world simulation
		List<List<String>> cols = new ArrayList<>();
		cols.add(Arrays.asList("Column 0", "Column 1"));
		cols.add(Arrays.asList("Column 0", "Column 1", "Column 2", "Column 3"));
		cols.add(Arrays.asList("Column 0", "Column 1", "Column 2", "Column 3", "Column 4", "Column 5", "Column 6", "Column 7", "Column 8", "Column 9"));
		double[] probGroup = {0.8, 0.1, 0.1};

		System.out.println("Running hybrid");
		TransactionTestResult test3 = testGroupFunction(cols, numRecords, 5000, probGroup, hybrid, qp, password);
		System.out.println("The Time was: " + test3.getTime());
		System.out.println("Throughput: " + test3.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(test3.getThroughput().get(i));
		}
		System.out.println("Running row");
		TransactionTestResult test4 = testGroupFunction(cols, numRecords, 5000, probGroup, row, qp, password);
		System.out.println("The Time was: " + test4.getTime());
		System.out.println("Throughput: " + test4.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(test4.getThroughput().get(i));
		}

		// Test 3 -- Real world simulation with writes
		// 0. 100% writes 0% reads
		System.out.println("Running hybrid");
		TransactionTestResult testGroup0_h = testWorkloadFunction(true, columns, cols, 5000, 1.0, 0.0, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup0_h.getTime());
		System.out.println("Throughput: " + testGroup0_h.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup0_h.getThroughput().get(i));
		}
		System.out.println("Running row");
		TransactionTestResult testGroup0_r = testWorkloadFunction(false, columns, cols, 5000, 1.0, 0.0, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup0_r.getTime());
		System.out.println("Throughput: " + testGroup0_r.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup0_r.getThroughput().get(i));
		}
		// 1. 80% writes 20% reads
		System.out.println("Running hybrid");
		TransactionTestResult testGroup1_h = testWorkloadFunction(true, columns, cols, 5000, 0.8, 0.2, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup1_h.getTime());
		System.out.println("Throughput: " + testGroup1_h.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup1_h.getThroughput().get(i));
		}
		System.out.println("Running row");
		TransactionTestResult testGroup1_r = testWorkloadFunction(false, columns, cols, 5000, 0.8, 0.2, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup1_r.getTime());
		System.out.println("Throughput: " + testGroup1_r.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup1_r.getThroughput().get(i));
		}
		// 2. 60% writes 40% reads
		System.out.println("Running hybrid");
		TransactionTestResult testGroup2_h = testWorkloadFunction(true, columns, cols, 5000, 0.6, 0.4, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup2_h.getTime());
		System.out.println("Throughput: " + testGroup2_h.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup2_h.getThroughput().get(i));
		}
		System.out.println("Running row");
		TransactionTestResult testGroup2_r = testWorkloadFunction(false, columns, cols, 5000, 0.6, 0.4, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup2_r.getTime());
		System.out.println("Throughput: " + testGroup2_r.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup2_r.getThroughput().get(i));
		}
		// 3. 40% writes 60% reads
		System.out.println("Running hybrid");
		TransactionTestResult testGroup3_h = testWorkloadFunction(true, columns, cols, 5000, 0.4, 0.6, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup3_h.getTime());
		System.out.println("Throughput: " + testGroup3_h.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup3_h.getThroughput().get(i));
		}
		System.out.println("Running row");
		TransactionTestResult testGroup3_r = testWorkloadFunction(false, columns, cols, 5000, 0.4, 0.6, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup3_r.getTime());
		System.out.println("Throughput: " + testGroup3_r.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup3_r.getThroughput().get(i));
		}
		// 4. 20% writes 80% reads
		System.out.println("Running hybrid");
		TransactionTestResult testGroup4_h = testWorkloadFunction(true, columns, cols, 5000, 0.2, 0.8, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup4_h.getTime());
		System.out.println("Throughput: " + testGroup4_h.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup4_h.getThroughput().get(i));
		}
		System.out.println("Running row");
		TransactionTestResult testGroup4_r = testWorkloadFunction(false, columns, cols, 5000, 0.2, 0.8, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup4_r.getTime());
		System.out.println("Throughput: " + testGroup4_r.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup4_r.getThroughput().get(i));
		}
		// 5. 0% writes 100% reads
		System.out.println("Running hybrid");
		TransactionTestResult testGroup5_h = testWorkloadFunction(true, columns, cols, 5000, 0.0, 1.0, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup5_h.getTime());
		System.out.println("Throughput: " + testGroup5_h.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup5_h.getThroughput().get(i));
		}
		System.out.println("Running row");
		TransactionTestResult testGroup5_r = testWorkloadFunction(false, columns, cols, 5000, 0.0, 1.0, probGroup, metadata, qp, password);
		System.out.println("The Time was: " + testGroup5_r.getTime());
		System.out.println("Throughput: " + testGroup5_r.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(testGroup5_r.getThroughput().get(i));
		}

		updater.shutdown();
		return;
	}

	public static TransactionTestResult testWorkloadFunction(boolean hybrid, List<String> cols, List<List<String>> colGroups, int numTxns, double read, double write, double[] probabilities, Metadata metadata, QueryProcessor qp, String password) throws IOException {

		int numRecords = 10000;
		int recordLength = 100;
		long startTime = 0;
		long estimatedTime = 0;
		int numCols = cols.size();
		int numColGroups = colGroups.size();

		Table table;
		if (hybrid) {
			table = metadata.createTable("Workload" + read + "-" + write, cols);
		} else {
			table = metadata.createTable("Workload" + read + "-" + write, cols, false);
		}
		tablePopulator(table, qp, recordLength, numRecords);

		// normalize probabilities
		double sum = read + write;
		read = read / sum;
		write = write / sum;

		Random random = new Random();
		int idToSearch;
		boolean isRead;
		int colGroupToRead;
		int i;
		
		ThroughputCounter throughput = new ThroughputCounter();
		throughput.start();
		for (i = 0; i < numTxns; i++) {
			isRead = pickReadWrite(read, write);
			if (isRead) {
				colGroupToRead = pickWeightedColumn(numColGroups, probabilities);
				idToSearch = random.nextInt(numRecords) + 1;
				startTime = System.nanoTime();
				qp.read(table, idToSearch, colGroups.get(colGroupToRead));
				throughput.increment();
				estimatedTime += System.nanoTime() - startTime;
			} else {
				Map<String, String> data = new HashMap<>();
				for (String col : cols) {
					String value = randomRecord(recordLength);
					data.put(col, value);
				}

				// Write record to file
				Record record = new Record(table, data);
				startTime = System.nanoTime();
				qp.write(record);
				throughput.increment();
				estimatedTime += System.nanoTime() - startTime;
				numRecords++;
			}
			if (password.length() > 0) {
				// ClearCache.clear(password);
			}
		}

		List<Integer> throughput_results = throughput.stopAndReturnThroughput();
		return new TransactionTestResult(throughput_results, estimatedTime);
	}

	public static boolean pickReadWrite(double read, double write) {

		Random random = new Random();
		int pick = random.nextInt(100);
		if (pick >= (read * 100)) {
			return true;
		} else {
			return false;
		}
	}

	public static TransactionTestResult testGroupFunction(List<List<String>> cols, int numRecords, int numReads, double[] probabilities, Table table, QueryProcessor qp, String password) throws IOException {

		long startTime = 0;
		long estimatedTime = 0;
		int numCols = cols.size();
		ThroughputCounter throughput = new ThroughputCounter();
		throughput.start();

		// check if number of column groups is equal to length of the probabilities input
		if (probabilities.length != numCols) {
			System.out.println("Length of probabilities input does not match number of column groups.");
			return new TransactionTestResult();
		}

		// normalize the array of probabilities
		double probabilitySum = 0;
		int i;
		for (i = 0; i < probabilities.length; i++) {
			probabilitySum += probabilities[i];
		}
		double temp; //probably don't need this
		for (i = 0; i < probabilities.length; i++) {
			temp = probabilities[i] / probabilitySum;
			probabilities[i] = temp;
		}

		Random random = new Random();
		int idToSearch;
		int colGroupToRead;

		for (i = 0; i < numReads; i++) {
			colGroupToRead = pickWeightedColumn(numCols, probabilities);
			idToSearch = random.nextInt(numRecords) + 1;
			startTime = System.nanoTime();
			qp.read(table, idToSearch, cols.get(colGroupToRead));
			throughput.increment();
			estimatedTime += System.nanoTime() - startTime;
			if (password.length() > 0) {
				// ClearCache.clear(password);
			}
		}

		List<Integer> throughput_results = throughput.stopAndReturnThroughput();
		return new TransactionTestResult(throughput_results, estimatedTime);
	}

	// More complicated script for testing transactions
	// numCols is the number of columns that we can choose to sample from
	// colsPerQuery is how many columns each query in the transaction will look up
	// numReads is how many records we are reading
	// probabilities is the array of weights we are sampling the numCols from
	public static TransactionTestResult testFunction(int numCols, int colsPerQuery, int numRecords, int numReads, double[] probabilities, Table table, QueryProcessor qp, String password) throws IOException {

		long startTime = 0;
		long estimatedTime = 0;
		ThroughputCounter throughput = new ThroughputCounter();
		throughput.start();

		// check if number of columns is equal to length of the probabilities input
		if (probabilities.length != numCols) {
			System.out.println("Length of probabilities input does not match number of columns.");
			return new TransactionTestResult();
		}
		if (colsPerQuery > numCols) {
			System.out.println("The number of columns per query should not exceed the number of columns sampled from.");
			return new TransactionTestResult();
		}

		// normalize the array of probabilities
		double probabilitySum = 0;
		int i;
		for (i = 0; i < probabilities.length; i++) {
			probabilitySum += probabilities[i];
		}
		double temp; //probably don't need this
		for (i = 0; i < probabilities.length; i++) {
			temp = probabilities[i] / probabilitySum;
			probabilities[i] = temp;
		}

		Random random = new Random();
		int idToSearch;
		List<String> colsToSearch;

		for (i = 0; i < numReads; i++) {
			colsToSearch = new ArrayList<>();
			int columnToRead;
			for (int j = 0; j < colsPerQuery; j++) {
				columnToRead = pickWeightedColumn(numCols, probabilities);
				colsToSearch.add("Column " + columnToRead);
			}
			idToSearch = random.nextInt(numRecords) + 1;
			startTime = System.nanoTime();
			qp.read(table, idToSearch, colsToSearch);
			throughput.increment();
			estimatedTime += System.nanoTime() - startTime;
			if (password.length() > 0) {
				// ClearCache.clear(password);
			}
		}

		List<Integer> throughput_results = throughput.stopAndReturnThroughput();
		return new TransactionTestResult(throughput_results, estimatedTime);
	}

	public static int pickWeightedColumn(int numCols, double[] probabilities) {
		double randomNumber = Math.random();
		double cumulativeProbability = 0.0;
		for (int i = 0; i < numCols; i++) {
			cumulativeProbability += probabilities[i];
			if (randomNumber < cumulativeProbability) {
				return i;
			}
		}
		return 0;
	}

	// Returns a random String of length input
	public static String randomRecord(int length) {

		StringBuilder sb = new StringBuilder(length);
		Random random = new Random();

		for (int i = 0; i < length; i++){
			sb.append(SEEDSTRING.charAt(random.nextInt(SEEDSTRING.length())));
		}
		return sb.toString();
	}

	// Writes records of random strings into the provided table
	public static void tablePopulator(Table table, QueryProcessor qp, int recordLength, int numRecords) throws IOException {

		for (int i = 0; i < numRecords; i++) {
			String value;
			List<String> columns = table.getColumns();
			Map<String, String> data = new HashMap<>();

			for (String column : columns) {
				value = randomRecord(recordLength);
				data.put(column, value);
			}

			// Write record to file
			Record record = new Record(table, data);
			qp.write(record);
		}
	}

	public static boolean deleteDirectory(File directory) {
		if(directory.exists()){
			File[] files = directory.listFiles();
			if(null!=files){
				for(int i=0; i<files.length; i++) {
					if(files[i].isDirectory()) {
						deleteDirectory(files[i]);
					}
					else {
						files[i].delete();
					}
				}
			}
		}
		return(directory.delete());
	}
}
