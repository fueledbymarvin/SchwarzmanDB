import java.util.*;

/**
 * Created by marvin on 4/7/16.
 */
public class Test {

    public static void main(String[] args) throws Exception {

//        Metadata metadata = new Metadata("/Users/frankjwu/Downloads/");
        Metadata metadata = new Metadata("/home/marvin/Downloads/", "sample");
        Queue<Update> updateQueue = new LinkedList<>();
        QueryProcessor qp = new QueryProcessor(updateQueue);
        ProjectionUpdater updater = new ProjectionUpdater(updateQueue, qp);
        updater.start();

        List<String> columns = new ArrayList<>();
        columns.add("first_name");
        columns.add("last_name");
        metadata.createTable("people", columns);

        List<String> columns2 = new ArrayList<>();
        columns2.add("item");
        columns2.add("stock");
        metadata.createTable("inventory", columns2, false);

        Metadata loaded = Metadata.load("/home/marvin/Downloads/sample");
        Table ppl = loaded.get("people");
        System.out.println(ppl.getColumns());
//        Table table = metadata.get("people");
//        Map<String, String> data = new HashMap<>();
//        data.put("first_name", "Marvin");
//        data.put("last_name", "Qian");
//        qp.write(new Record(table, data));
//        data.put("first_name", "Frank");
//        data.put("last_name", "Wu");
//        qp.write(new Record(table, data));
//        data.put("first_name", "Justin");
//        data.put("last_name", "Zhang");
//        qp.write(new Record(table, data));
//
//        System.out.println("Scan");
//        List<Record> records = qp.scan(table, columns);
//        for (Record record : records) {
//            System.out.println(record.getId() + ": " + record.getValues());
//        }
//
//        System.out.println("\nSingle read");
//        Record record = qp.read(table, 3, columns);
//        System.out.println(record.getId() + ": " + record.getValues());
        updater.shutdown();
        updater.join();
    }
}
