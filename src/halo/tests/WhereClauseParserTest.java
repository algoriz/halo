package halo.tests;

import halo.client.ColumnFilterTree;
import halo.client.WhereClause;

/**
 * Unit test for WhereClauseParser
 */
public class WhereClauseParserTest {
    private static String printFilterTree(ColumnFilterTree node) {
        if (node.isLeafNode()) {
            return node.getColumnFilter().getColumn();
        }
        return "(" + printFilterTree(node.getLeftChild())
                + (node.getConnector() == 0 ? " || " : " && ")
                + printFilterTree(node.getRightChild()) + ")";
    }

    private static boolean testCase(String stmt, String expected){
        try {
            WhereClause clause = WhereClause.parse(stmt, 0, stmt.length());
            String result = printFilterTree(clause.getFilterTree());
            if (result.equals(expected)){
                return true;
            }
            System.out.println("WhereClauseParserTest failed at case: " + stmt
                    + ", returns " + result + ", but expecting " + expected);
        } catch (Exception e) {
            System.out.println("WhereClauseParserTest failed at case: " + stmt
                    + ", exception details: ");
            e.printStackTrace(System.out);
        }
        return false;
    }

    public static void main(String[] args) {
        boolean pass = true;
        pass = testCase("where name = \"jack\" and score = 87",
                "(name && score)") && pass;
        pass = testCase("  where name = \"jack\" and score = 87;;  ;",
                "(name && score)") && pass;
        pass = testCase("where (name = \"jack\") and ( score = 87 )",
                "(name && score)") && pass;
        pass = testCase("where (name = \"jack\"  and  score = 87 )",
                "(name && score)") && pass;
        pass = testCase("where (c1 = \"jack\"  and  c2 = 2 ) or c3>\"\\\\\\\"\" and c4<=\"2014-01-01 00:00:00\"",
                "((c1 && c2) || (c3 && c4))") && pass;
        pass = testCase("where c1==1 or c2!=2 and c3<=3 and c4>=4 or c5 <5 and c6==6 or c7==\"\"",
                "(((c1 || ((c2 && c3) && c4)) || (c5 && c6)) || c7)") && pass;
        pass = testCase("where c1==1 or c2!=2; and c3<=3 and c4>=4 or c5 <5 and c6==6 or c7==\"\"",
                "(c1 || c2)") && pass;
        pass = testCase("where c1 between 1 and 5 or c2 between 2 and 6 and c3 between \"\" and \"1000\";",
                "(c1 || (c2 && c3))") && pass;
        if (!pass) {
            System.out.println("*** WhereClauseParserTest FAILED!!!");
        }
    }
}
