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
		 deleteDirectory(new File("/Users/frankjwu/Downloads/test"));
		 Metadata metadata = new Metadata("/Users/frankjwu/Downloads/", "test");
//		deleteDirectory(new File("/home/marvin/Downloads/test"));
//		Metadata metadata = new Metadata("/home/marvin/Downloads/", "test");
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
		System.out.println("The Transaction Time was: " + test1.getTime());
		System.out.println("Throughput: " + test1.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(test1.getThroughput().get(i));
		}
		System.out.println("Running row");
		TransactionTestResult test2 = testFunction(10, 1, numRecords, 5000, prob, row, qp, password);
		System.out.println("The Transaction Time was: " + test2.getTime());
		System.out.println("Throughput: " + test2.getThroughput());
		for (int i = 0; i < 50; i++) {
			System.out.println(test2.getThroughput().get(i));
		}

		// Test 2 -- Transaction 1 accesses a increasingly larger group of columns 10 times more than the others. This
		// will result in new projections being created. The goal is to see performance of differently-sized projections
		// and when, in the current system, it doesn't make sense to create one.
		double[] prob3 = {10, 10, 1, 1, 1, 1, 1, 1, 1, 1};
		TransactionTestResult test3 = testFunction(10, 2, numRecords, 1000, prob3, table, qp, password);
		System.out.println("The Transaction Time was: " + test3.getTime());
		System.out.println("Throughput: " + test3.getThroughput());
		double[] prob4 = {10, 10, 1, 1, 1, 1, 1, 1, 1, 1};
		TransactionTestResult test4 = testFunction(10, 2, numRecords, 1000, prob4, table, qp, password);
		System.out.println("The Transaction Time was: " + test4.getTime());
		System.out.println("Throughput: " + test4.getThroughput());
		double[] prob5 = {10, 10, 10, 1, 1, 1, 1, 1, 1, 1};
		TransactionTestResult test5 = testFunction(10, 3, numRecords, 1000, prob5, table, qp, password);
		System.out.println("The Transaction Time was: " + test5.getTime());
		System.out.println("Throughput: " + test5.getThroughput());
		double[] prob6 = {10, 10, 10, 10, 1, 1, 1, 1, 1, 1};
		TransactionTestResult test5 = testFunction(10, 4, numRecords, 1000, prob6, table, qp, password);
		System.out.println("The Transaction Time was: " + test6.getTime());
		System.out.println("Throughput: " + test6.getThroughput());
		double[] prob7 = {10, 10, 10, 10, 10, 1, 1, 1, 1, 1};
		TransactionTestResult test7 = testFunction(10, 5, numRecords, 1000, prob7, table, qp, password);
		System.out.println("The Transaction Time was: " + test7.getTime());
		System.out.println("Throughput: " + test7.getThroughput());
		double[] prob8 = {10, 10, 10, 10, 10, 10, 1, 1, 1, 1};
		TransactionTestResult test8 = testFunction(10, 6, numRecords, 1000, prob8, table, qp, password);
		System.out.println("The Transaction Time was: " + test8.getTime());
		System.out.println("Throughput: " + test8.getThroughput());
		double[] prob9 = {10, 10, 10, 10, 10, 10, 10, 1, 1, 1};
		TransactionTestResult test9 = testFunction(10, 7, numRecords, 1000, prob9, table, qp, password);
		System.out.println("The Transaction Time was: " + test9.getTime());
		System.out.println("Throughput: " + test9.getThroughput());
		double[] prob10 = {10, 10, 10, 10, 10, 10, 10, 10, 1, 1};
		TransactionTestResult test10 = testFunction(10, 8, numRecords, 1000, prob10, table, qp, password);
		System.out.println("The Transaction Time was: " + test10.getTime());
		System.out.println("Throughput: " + test10.getThroughput());
		double[] prob11 = {10, 10, 10, 10, 10, 10, 10, 10, 10, 1};
		TransactionTestResult test11 = testFunction(10, 9, numRecords, 1000, prob11, table, qp, password);
		System.out.println("The Transaction Time was: " + test11.getTime());
		System.out.println("Throughput: " + test11.getThroughput());

		updater.shutdown();
		return;
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
