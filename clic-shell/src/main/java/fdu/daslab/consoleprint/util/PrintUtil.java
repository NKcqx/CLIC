package fdu.daslab.consoleprint.util;

import fdu.daslab.consoleprint.enums.Align;
import fdu.daslab.consoleprint.table.Cell;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 引用自：https://github.com/clyoudu/clyoudu-util
 */
public class PrintUtil {
    private static int len = 35;

    /**
     * print sep line
     *
     * @param columnWidths  max width of each column
     * @param horizontalSep char of h-sep, default '-'
     * @param verticalSep   char of v-sep, default '|'
     * @param joinSep       char of corner, default '+'
     */
    public static List<String> printLineSep(int[] columnWidths, String horizontalSep, String verticalSep, String joinSep) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < columnWidths.length; i++) {
            String l = String.join("", Collections.nCopies(columnWidths[i]
                    + StringPadUtil.strLength(verticalSep) + 1, horizontalSep));
            line.append(joinSep).append(l).append(i == columnWidths.length - 1 ? joinSep : "");
        }
        return Collections.singletonList(line.toString());
    }

    /**
     * print real data rows
     *
     * @param rows         data rows
     * @param columnWidths max width of each column
     * @param verticalSep  char of v-sep, default '|'
     */
    public static List<String> printRows(List<List<Cell>> rows, int[] columnWidths, String verticalSep) {
        List<String> result = new ArrayList<>();
        for (List<Cell> row : rows) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.size(); i++) {
                Cell cell = row.get(i);
                if (cell == null) {
                    cell = new Cell("");
                }
                //add v-sep after last column
                String verStrTemp = i == row.size() - 1 ? verticalSep : "";
                Align align = cell.getAlign();
                switch (align) {
                    case LEFT:
                        sb.append(String.format("%s %s %s", verticalSep, StringPadUtil.rightPad(cell.getValue(), columnWidths[i]), verStrTemp));
                        break;
                    case RIGHT:
                        sb.append(String.format("%s %s %s", verticalSep, StringPadUtil.leftPad(cell.getValue(), columnWidths[i]), verStrTemp));
                        break;
                    case CENTER:
                        sb.append(String.format("%s %s %s", verticalSep, StringPadUtil.center(cell.getValue(), columnWidths[i]), verStrTemp));
                        break;
                    default:
                        throw new IllegalArgumentException("wrong align : " + align.name());
                }
            }
            result.add(sb.toString());
        }
        return result;
    }

    public static String processOutLen(String str) {
        //默认长度为len
        String res = processOutLen(str, len);
        return res;
    }

    public static String processOutLen(String str, int len) {
        String res = null;
        if (Math.min(str.length(), len) == str.length()) {
            return str;
        } else {
            res = str.substring(0, len) + "...";
        }
        return res;
    }
}
