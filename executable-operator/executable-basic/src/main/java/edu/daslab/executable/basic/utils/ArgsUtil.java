package edu.daslab.executable.basic.utils;

import com.beust.jcommander.JCommander;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 参数解析的工具类
 */
public class ArgsUtil {

    /**
     * 通过某个key参数分割参数集合，并按顺序返回
     *
     * @param args 参数列表
     * @param keyArg 关键字
     * @return 有序组织的 关键字对应的value - 参数列表
     */
    public static Map<String, String[]> separateArgsByKey(String[] args, String keyArg) {
        // 需要保证插入顺序和实际顺序一致
        Map<String, String[]> separateMaps = new LinkedHashMap<>();
        int i = 0;
        while (i < args.length && args[i].startsWith(keyArg)) {
            // 下一个分割位置
            int nextIndex = i + 1;
            while (nextIndex < args.length && !args[nextIndex].startsWith(keyArg)) nextIndex++;
            separateMaps.put(args[i].substring(keyArg.length() + 1), Arrays.copyOfRange(args, i + 1, nextIndex));
            i = nextIndex;
        }
        return separateMaps;
    }

    /**
     * 解析命令行参数
     *
     * @param argObject 解析到的对象
     * @param args 输入参数
     */
    public static void parseArgs(Object argObject, String[] args) {
        JCommander.newBuilder()
                .addObject(argObject)
                .build()
                .parse(args);
    }
}
