import com.monchickey.HbaseUtil;

import java.util.HashMap;
import java.util.Map;

public class HBaseTest {
    public static void main(String[] args) {
        String tableName = "empTable";

        System.out.println(HbaseUtil.addColumn(tableName, "test1",
                "cf", "name", "xv1"));

        Map<String, Object> cols = new HashMap<String, Object>();
        cols.put("col1", "kk");
        cols.put("col2", 201523929288292L);
        cols.put("col3", 23);
        cols.put("col4", 11.5f);
        cols.put("col5", 21.2);
        cols.put("col6", true);
        cols.put("col7", (short) 1024);
        cols.put("col8", "byte array".getBytes());
        System.out.println(HbaseUtil.addColumns(tableName, "test2", "cf", cols));
    }
}
