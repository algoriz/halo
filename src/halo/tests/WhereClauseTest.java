package halo.tests;

import halo.common.Bytes;
import halo.core.*;
import halo.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.util.Date;

/**
 * Created by riz on 14-7-14.
 */
public class WhereClauseTest {
    public static byte[][] makeValues(int n, int columns){
        byte[][] values = new byte[columns][];
        for (int i = 0; i < columns; ++i){
            values[i] = Bytes.toBytes(n+i+1);
        }
        return values;
    }

    public static HaloTable prepareTestData() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HaloAdmin admin = new HaloAdmin(conf);

        int columns = 4;
        HaloTable table;
        if (admin.tableExists("t2")){
            admin.dropTable("t2");
        }

        {
            ColumnProperty[] cols = new ColumnProperty[columns];
            for (int i = 0; i < columns; ++i){
                cols[i] = new ColumnProperty("c" + i, DataType.INT32, true);
            }
            TableProperty prop = new TableProperty("t2", cols);
            prop.setCreateDate(new Date());
            prop.setOwner("wctest");
            admin.createTable(prop);

            table = admin.openTable("t2");
        }

        int[] selectedCols = new int[columns];
        for (int i = 0; i < columns; ++i){
            selectedCols[i] = i;
        }

        for (int i = 0; i < 1000; ++i) {
            table.insert(selectedCols, makeValues(i, columns));
        }

        return table;
    }

    public static void main(String[] args) {
        try {
            //HaloTable table = prepareTestData();
            Configuration conf = HBaseConfiguration.create();
            HaloAdmin admin = new HaloAdmin(conf);
            HaloTable table = admin.openTable("t2");

            int[] selectedCols = new int[1];
            selectedCols[0] = 0;
            WhereClause wc = WhereClause.parse("WHERE c0 BETWEEN 10 AND 110 and (c0 > 100 or c0 < 20);");
            RowSet rowSet = wc.applyToTable(table);
            QueryResult result = table.select(selectedCols, rowSet);

            int rows = result.getNumberOfRows();
            if (rows != 20){
                System.out.println("*** WhereClause test failed!");
                return;
            }

            int first = Bytes.toInt(result.getRow(0)[0]);
            if (first != 10){
                System.out.println("*** WhereClause test failed!");
            }

            int last = Bytes.toInt(result.getRow(rows - 1)[0]);
            if (last != 110){
                System.out.println("*** WhereClause test failed!");
            }

            for (int i = 0; i < rows; ++i){
                System.out.println(Bytes.toInt(result.getRow(i)[0]));
            }
        }
        catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }
}
