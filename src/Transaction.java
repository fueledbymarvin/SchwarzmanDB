import java.util.*;

/**
 * Created by Frank on 4/15/16.
 */
public class Transaction {
    List<String> queries; // list of queries in the following format: table,id,columns

    public Transaction(List<String> queries) {

        this.queries = queries;
    }

    // Simulates a transaction -- pass in the ids and the columns of data requested
    public List<Record> run() {

        List<Record> records = new ArrayList<>();

        for (String query : queries) {
            List<String> splitQuery = new ArrayList<>();
            if (!query.isEmpty()) {
                splitQuery = Arrays.asList(query.split(","));

                // table:  splitQuery.get(0)
                // id:     splitQuery.get(1)
                // column: splitQuery.get(2)

                // open table file using column info
                // find id in that file
            }
        }
        return records;
    }
}
