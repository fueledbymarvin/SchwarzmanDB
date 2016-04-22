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
		// Transaction One: Speed of accessing one column repeatedly
        int idToSearch;
        List<String> colsToSearch = new ArrayList<>();
        Random random = new Random();

        System.out.println("Performing Test 1...");
        estimatedTime = 0;
        colsToSearch.add("Column 1");
        for (int i = 0; i < 105; i++) {
            idToSearch = random.nextInt(numRecords) + 1;
			// System.out.println(idToSearch);
            startTime = System.nanoTime();
            qp.read(table, idToSearch, colsToSearch);
            estimatedTime += System.nanoTime() - startTime;
			// System.out.println(i);
        }
        System.out.println("The Total Elapsed Time was: " + estimatedTime + "\n");
		
		// Transaction Two: Speed of random accessing of columns
        System.out.println("Performing Test 2...");
        estimatedTime = 0;
        for (int i = 0; i < 50; i++) {
            colsToSearch = new ArrayList<>();
            colsToSearch.add("Column " + random.nextInt(numCols));
            idToSearch = random.nextInt(numRecords) + 1;
//            System.out.println(idToSearch);
            startTime = System.nanoTime();
            qp.read(table, idToSearch, colsToSearch);
            estimatedTime += System.nanoTime() - startTime;
//            System.out.println(i);
        }
        System.out.println("The Total Elapsed Time was: " + estimatedTime);
		
		
		//surround statements with this code in order to get the time
//		long startTime = System.nanoTime();
		
//		long estimatedTime = System.nanoTime() - startTime;
//		System.out.println("The Total Elapsed Time was :" + estimatedTime);
		
		//
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
