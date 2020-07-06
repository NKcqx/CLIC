package basic;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author 刘丰艺
 * @since 2020/7/6 14:05
 * @version 1.0
 */
public class CompileUdfClass {

    public static void main(String[] args) {
        try{
            String shPath = "shell/compile_udf_class.sh";
            String cmd = "cmd \\c ./" + shPath;
            Process ps = Runtime.getRuntime().exec(cmd);
            //ps.waitFor();

            System.out.println("----------------------------");
            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while((line = br.readLine()) != null) {
                sb.append(line).append("\n");
                System.out.println(line);
            }
            String result = sb.toString();
            System.out.println("result: " + result);
            br.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
