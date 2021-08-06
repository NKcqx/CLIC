package basic;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/6 14:05
 */
public class CompileUdfClass {

    public static void main(String[] args) {
        try {
            String shPath = "shell/compile_udf_class.sh";
            String cmd = "cmd \\c ./" + shPath;
            Process ps = Runtime.getRuntime().exec(cmd);
            //ps.waitFor();

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            String result = sb.toString();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
