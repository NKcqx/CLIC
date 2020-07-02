package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java平台的join算子
 */
@Parameters(separators = "=")
public class JoinOperator implements BinaryBasicOperator<Stream<List<String>>, Stream<List<String>>, Stream<List<String>>> {

    @Parameter(names={"--rightTableKeyName"})
    String rightTableKeyExtractFunctionName;

    @Parameter(names={"--leftTableKeyName"})
    String leftTableKeyExtractFunctionName;

    @Parameter(names={"--rightTableFuncName"})
    String rightTableFuncName;

    @Parameter(names={"--leftTableFuncName"})
    String leftTableFuncName;

    @Override
    public void execute(ParamsModel<Stream<List<String>>> inputArgs,
                        ResultModel<Stream<List<String>>> input1,
                        ResultModel<Stream<List<String>>> input2) {
        JoinOperator joinArgs = (JoinOperator) inputArgs.getOperatorParam();
        FunctionModel joinFunction = inputArgs.getFunctionModel();
        assert joinFunction != null;
        // 新右表(用户指定join的时候右表要select哪几列)
        Stream<List<String>> newRightTable = input2.getInnerResult()
                .map(data -> (List<String>) joinFunction.invoke(joinArgs.rightTableFuncName, data));
        // 新左表(用户指定join的时候左表要select哪几列)
        Stream<List<String>> newLeftTable = input1.getInnerResult()
                .map(data -> (List<String>) joinFunction.invoke(joinArgs.leftTableFuncName, data));
        // 右表转化成Map(把用户定义的join key提取出来作为Map的key)
        // 该Map已去重复key(key相等则对应的value取代该key的旧value)
        Map<String, List<String>> rightTableMap = newRightTable
                .collect(Collectors.toMap(
                        data -> (String) joinFunction.invoke(joinArgs.rightTableKeyExtractFunctionName, data),
                        value -> value,
                        (value1, value2) -> value1
                        ));
        // 左表转化成Map(把用户定义的join key提取出来作为Map的key)
        // 该Map已去重复key(key相等则对应的value取代该key的旧value)
        Map<String, List<String>> leftTableMap = newLeftTable
                .collect(Collectors.toMap(
                        data -> (String) joinFunction.invoke(joinArgs.leftTableKeyExtractFunctionName, data),
                        value -> value,
                        (value1, value2) -> value1
                ));

//        // 左表join右表(Nested Loop Join) (可用)
//        List<String> resultLine = new ArrayList<>();
//        List<String> resultList = new ArrayList<>();
//        for(Map.Entry<String, List<String>> entry1 : leftTableMap.entrySet()) {
//            for(Map.Entry<String, List<String>> entry2 : rightTableMap.entrySet()) {
//                if(entry1.getKey().equals(entry2.getKey())) {
//                    resultLine.addAll(entry1.getValue());
//                    resultLine.addAll(entry2.getValue());
//                }
//            }
//            resultList.add(String.join(",",resultLine));
//            resultLine.clear();
//        }
//        Stream<List<String>> nextStream = (Stream<List<String>>) resultList;
//        input1.setInnerResult(nextStream);

        // 左表join右表(Hash Join) (暂未可用)
        // 哈希表的装填因子一般设为0.75
        double loadFactory = 0.75;
        int hashTableLength = 0;
        int primeNumber = 0;
        // 确定大表和小表，小表作Build Table，大表作Probe Table
        if(rightTableMap.size() < leftTableMap.size()) {
            // 确定哈希表表长
            hashTableLength = (int) Math.ceil(rightTableMap.size() / loadFactory);
            // 除留取余法，模数取不大于表长的最大质数，先用23代替
            primeNumber = 23;
            HashTable hashTable = new HashTable(hashTableLength);
            // 遍历rightTableHash，将其按key值构建hashTable
            // 遍历leftTableHash，找到hashcode相同的桶，再匹配right和left的key是否相等，相等则该项join成功
        } else {

        }

    }

    // 拉链法构建哈希表
    public static class HashTable {
        public class Node {
            Node next;
            Object key;
            Object data;

            public Node(Object key, Object data) {
                this.key = key;
                this.data = data;
            }
        }
        public Node[] Headlist;
        // 当前哈希表元素个数
        public int size = 0;

        public HashTable(int size) {
            this.size = size;
            Headlist = new Node[size];
        }

        // hash函数计算hashcode，即头结点位置
        private Integer hash(Object key, int length) {
            Integer index = null;
            if(key!=null) {
                // key值如果不是数字，先转化成字符数组
                char[] charList = key.toString().toCharArray();
                int number = 0;
                // 依次计算每个字符对应的ASCII码
                for(int i=0;i<charList.length;i++) {
                    number += charList[i];
                }
                // 对哈希表数组长度取余得到头结点位置
                index = Math.abs(number % length);
            }
            return index;
        }

        public void put(Object key, Object data) {
            int index = hash(key, Headlist.length);
            // 当前节点封装成Node节点
            Node node = new Node(key, data);
            // 加入哈希表
            input(node, Headlist, index);
            size++;
        }

        // 添加函数
        private void input(Node node, Node[] list, int index) {
            // 若头结点位置为空，则把当前节点赋值给头节点
            if(list[index] == null ) {
                list[index] = node;
            } else { // 否则遍历该链表并判断该键值是否已存在于哈希表中，没有就将其加到链表尾部
                Node temp = list[index];
                // 判断表头元素的键值是否和即将输入的键值一样
                if(temp.key == node.key) {
                    System.out.println(temp.key+"--该键值已存在！");
                } else {
                    while (temp.next != null) {
                        temp = temp.next;
                        if(temp.key == node.key) {
                            System.out.println(temp.key+"--该键值已存在！");
                            break;
                        }
                    }
                    temp.next = node;
                }
            }
        }

        // 获取键值对应的数据
        public Object get(Object key) {
            // 获取key对应的hashcode
            int index = hash(key, Headlist.length);
            Node temp = Headlist[index];
            // 判断相对应的头结点是否为空
            if(temp == null) return null;
            else{
                // 判断节点中的key与待查找的key是否相同
                if(temp.key == key)
                    return temp.data;
                else{
                    while(temp.next != null) {
                        temp = temp.next;
                        if(temp.key == key)
                            return temp.data;
                    }
                }
            }
            return null;
        }

        // 返回当前哈希表大小
        public int length() {
            return size;
        }
    }
}
