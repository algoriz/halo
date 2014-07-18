package halo.tests;

import halo.common.Bytes;
import halo.core.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.util.Date;

public class Main {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HaloAdmin admin = new HaloAdmin(conf);
        if (admin.tableExists("t1")) {
            admin.dropTable("t1");
        }

        if (!admin.tableExists("t1")) {
            // CREATE TABLE t1 ( name varchar(16), score integer index );
            ColumnProperty[] cols = new ColumnProperty[2];
            cols[0] = new ColumnProperty("name", DataType.varchar(16), false);
            cols[1] = new ColumnProperty("score", DataType.INT32, true);
            TableProperty prop = new TableProperty("t1", cols);
            prop.setCreateDate(new Date());
            prop.setOwner("anonymous");
            admin.createTable(prop);
        }

        // insert into t1(name, score) values ('jack', 87)

        HaloTable table = admin.openTable("t1");
        int[] selectedCols = new int[2];
        selectedCols[0] = 0;
        selectedCols[1] = 1;

        {
            byte[][] values = new byte[2][];
            values[0] = "jack".getBytes();
            values[1] = Bytes.toBytes(87);
            table.insert(selectedCols, values);
            values[0] = "lucy".getBytes();
            values[1] = Bytes.toBytes(92);
            table.insert(selectedCols, values);
        }

        {
            RowSet rows = new RowSet();
            rows.add(RowId.valueOf(1));
            rows.add(RowId.valueOf(2));
            rows.add(RowId.valueOf(3));
            rows.add(RowId.valueOf(4));
            rows.add(RowId.valueOf(5));
            rows.add(RowId.valueOf(6));

            QueryResult result = table.select(selectedCols, rows);
            int numberOfRows = result.getNumberOfRows();
            for (int i = 0; i < numberOfRows; ++i) {
                byte[][] values = result.getRow(i);
                System.out.println("{" + Bytes.toString(values[0]) + "," + Bytes.toInt(values[1]) + "}");
            }

            table.delete(rows);
        }

        table.close();
    }
}
