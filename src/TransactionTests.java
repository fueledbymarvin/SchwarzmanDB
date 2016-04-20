package schwarzmanDB;
import java.util.Random;

public class TransactionTests {
	final static String SEEDSTRING = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	
	public static void main(String args[]) {
        int numCols = 10;
        int numRecords = 1000000;
		int recordLength = 100;
		// Generate Records
//        Metadata metadata = new Metadata("/Users/frankjwu/Downloads/");
        Metadata metadata = new Metadata("/home/marvin/Downloads/");
		List<String> columns = new ArrayList<>();
		for(int i = 0; i < numCols; i++){
        	columns.add("Column " + i);
        }
        metadata.createTable("Tupac", columns);
        Table table = metadata.get("Tupac");


        //Insert Random Records
        long startTime = System.nanoTime();
        tablePopulator(table, recordLength, numRecords);
		long estimatedTime = System.nanoTime() - startTime;
		System.out.println("The Total Elapsed Time was :" + estimatedTime);
		
		// First Test
		// With only one table
		// First set of transactions will test effectiveness of freshness algorithm
		// Does reading the same pairs of columns repeatedly together beat fetching 
		// random columns. How does the number of columns in primary file affect speed
		// Transaction One: Speed of accessing one column repeatedly
		
		// Transaction Two: Speed of random accessing of columns
			
		
		
		//surround statements with this code in order to get the time
		long startTime = System.nanoTime();
		
		long estimatedTime = System.nanoTime() - startTime;
		System.out.println("The Total Elapsed Time was :" + estimatedTime);
		
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
	
	public void tablePopulator(Table table, int recordLength, int numRecords) {
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
			Record record = new record(table, data);
			QueryProcessor.write(record);
		}
	}
}
