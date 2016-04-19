import java.io.IOException;
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
    public List<Record> run() throws IOException {

//        Metadata metadata = new Metadata("/Users/frankjwu/Downloads/");
        Metadata metadata = new Metadata("/home/marvin/Downloads/");
        List<Record> records = new ArrayList<>();
        Map<String, List<String>> primaryColsToFetch = new HashMap<>();
        Map<String, List<String>> secondaryColsToFetch = new HashMap<>();

        // Iterate through transaction's queries to figure out what files and columns are needed
        for (String query : queries) {
            List<String> splitQuery = new ArrayList<>();

            if (!query.isEmpty()) {
                splitQuery = Arrays.asList(query.split(","));
                String tableString  = splitQuery.get(0);
                String idString     = splitQuery.get(1);
                String columnString = splitQuery.get(2);
                Map<String, List<String>> addTo = new HashMap<>();

                Table table = metadata.get(splitQuery.get(0));
                if (table.isPrimary(splitQuery.get(1))) {
                    addTo = primaryColsToFetch;
                } else if (table.isSecondary(splitQuery.get(1))) {
                    addTo = secondaryColsToFetch;
                }

                List<String> idList;
                if ((idList = addTo.get(columnString)) == null) {
                    idList = new ArrayList<>();
                } else {
                    idList.add(idString);
                }
                addTo.put(columnString, idList);
            }
        }


        // Go through primary and secondary files
        records.addAll(searchFile(primaryColsToFetch));
        records.addAll(searchFile(secondaryColsToFetch));

        return records;
    }

    private List<Record> searchFile(Map<String, List<String>> colsToFetch) {

        List<Record> records = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : colsToFetch.entrySet()) {
            String columnString = entry.getKey();
            List<String> idList = entry.getValue();

            // TODO: open file and pull out appropriate values
        }

        return records;
    }
}


