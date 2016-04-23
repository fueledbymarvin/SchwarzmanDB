import java.io.IOException;
import java.util.*;

public class TransactionTests {

	final static String SEEDSTRING = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	
	public static void main(String args[]) throws IOException {

        Queue<Table> updateQueue = new LinkedList<>();
        QueryProcessor qp = new QueryProcessor(updateQueue);
        ColumnUpdater updater = new ColumnUpdater(updateQueue, qp);
        updater.start();

        int numCols = 10;
        int numRecords = 10000;
		int recordLength = 100;
		// Generate Records
        // Metadata metadata = new Metadata("/Users/frankjwu/Downloads/");
		Metadata metadata = new Metadata("/home/marvin/Downloads/");
		List<String> columns = new ArrayList<>();
		for(int i = 0; i < numCols; i++){
        	columns.add("Column " + i);
        }
        metadata.createTable("Tupac", columns);
        Table table = metadata.get("Tupac");

        //Insert Random Records
        System.out.println("Writing random records...");
        long startTime = System.nanoTime();
        tablePopulator(table, qp, recordLength, numRecords);
		long estimatedTime = System.nanoTime() - startTime;
		System.out.println("Total Elapsed Time was: " + estimatedTime + "\n");
		
		// First Test
		// With only one table
		// First set of transactions will test effectiveness of freshness algorithm
		// Does reading the same pairs of columns repeatedly together beat fetching 
		// random columns. How does the number of columns in primary file affect speed
        int idToSearch;
        List<String> colsToSearch = new ArrayList<>();
        Random random = new Random();

        // Transaction One: Speed of accessing one column repeatedly
        System.out.println("Performing Test 1...");
        estimatedTime = 0;
        colsToSearch.add("Column 1");
        for (int i = 0; i < 105; i++) {
            idToSearch = random.nextInt(numRecords) + 1;
			// System.out.println(idToSearch);
            startTime = System.nanoTime();
            qp.read(table, idToSearch, colsToSearch);
            estimatedTime += System.nanoTime() - startTime;
            ClearCache.clear(args[0]);
			// System.out.println(i);
        }
        System.out.println("Transaction 1 Time was: " + estimatedTime + "\n");

		// Transaction Two: Speed of random accessing of columns
        estimatedTime = 0;
        for (int i = 0; i < 50; i++) {
            colsToSearch = new ArrayList<>();
            colsToSearch.add("Column " + random.nextInt(numCols));
            idToSearch = random.nextInt(numRecords) + 1;
//            System.out.println(idToSearch);
            startTime = System.nanoTime();
            qp.read(table, idToSearch, colsToSearch);
            estimatedTime += System.nanoTime() - startTime;
            ClearCache.clear(args[0]);
//            System.out.println(i);
        }
        System.out.println("Transaction 2 Time was: " + estimatedTime);
		
		// Testing out testFunction script
		double[] probabilities = {0.5, 0.5};
		testFunction(2, 2, numRecords, 1000, probabilities, table, qp);
		//surround statements with this code in order to get the time
//		long startTime = System.nanoTime();
		
//		long estimatedTime = System.nanoTime() - startTime;
//		System.out.println("The Total Elapsed Time was :" + estimatedTime);
		
		//
	}

	// More complicated script for testing transactions
	// numCols is the number of columns that we can choose to sample from
	// colsPerQuery is how many columns each query in the transaction will look up
	// numReads is how many records we are reading
	// probabilities is the array of weights we are sampling the numCols from
	public static void testFunction(int numCols, int colsPerQuery, int numRecords, int numReads, double[] probabilities, Table table, QueryProcessor qp) {
		long estimatedTime = 0;
		// check if number of columns is equal to length of the probabilities input
		if (probabilities.length != numCols) {
			System.out.println("Length of probabilities input does not match number of columns.");
			return;
		}
		if (colsPerQuery > numCols) {
			System.out.println("The number of columns per query should not exceed the number of columns sampled from.");
			return;
		}
		// check if numCols is less than number of columns in table
		if (numCols > table.getNumCols()) {
			System.out.println("Table only has " + table.getNumCols() + " columns.");
			return;
		}

		// normalize the array of probabilities
		double probabilitySum = 0;
		for(i = 0; i < probabilities.length; i++) {
			probabilitySum += probabilities[i];
		}
		double temp; //probably don't need this
		for(i = 0; i < probabilities.length; i++) {
			temp = probabilities[i] / probabilitySum;
			probabilities[i] = temp;
		}


		for (int i = 0; i < numReads; i++) {
            colsToSearch = new ArrayList<>();
            int columnToRead;
            for(int j = 0; j < colsPerQuery; j++) {}
            	columnToRead = pickWeightedColumn(numCols, probabilities);
            	colsToSearch.add("Column " + columnToRead);
            }
            idToSearch = random.nextInt(numRecords) + 1;
//            System.out.println(idToSearch);
            startTime = System.nanoTime();
            qp.read(table, idToSearch, colsToSearch);
            estimatedTime += System.nanoTime() - startTime;
            ClearCache.clear(args[0]);
//            System.out.println(i);
        }
        System.out.println("The Transaction Time was : " + estimatedTime);
        return;
	}
	
	public int pickWeightedColumn(int numCols, double[] probabilities) {
		double randomNumber = Math.random();
		double cumulativeProbability = 0.0;
		for(int i = 0; i < numCols; i++) {
			cumulativeProbability += probabilities[i];
			if (randomNumber < cumulativeProbability) {
				return i;
			}
		}
	}
	// Returns a random String of length input
	public static String randomRecord(int length)	{

		StringBuilder sb = new StringBuilder(length);
		Random random = new Random();
		
		for(int i = 0; i < length; i++){
			sb.append(SEEDSTRING.charAt(random.nextInt(SEEDSTRING.length())));
		}
		return sb.toString();
	}
	
	public static void tablePopulator(Table table, QueryProcessor qp, int recordLength, int numRecords) throws IOException {

		for (int i = 0; i < numRecords; i++) {
			String value;
			
			List<String> primaryColumns = table.getPrimaryColumns();
			List<String> secondaryColumns = table.getSecondaryColumns();
			Map<String, String> data = new HashMap<>();

			for(String temp : primaryColumns) {
				value = randomRecord(recordLength);
				data.put(temp, value);
			}
			for(String temp : secondaryColumns) {
				value = randomRecord(recordLength);
				data.put(temp, value);
			}
			// put the record in yay!
			Record record = new Record(table, data);
			qp.write(record);
		}
	}
}
