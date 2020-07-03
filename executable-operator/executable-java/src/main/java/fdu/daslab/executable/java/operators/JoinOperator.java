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
                        key -> (String) joinFunction.invoke(joinArgs.rightTableKeyExtractFunctionName, key),
                        value -> value,
                        (value1, value2) -> value1
                        ));
        // 左表转化成Map(把用户定义的join key提取出来作为Map的key)
        // 该Map已去重复key(key相等则对应的value取代该key的旧value)
        Map<String, List<String>> leftTableMap = newLeftTable
                .collect(Collectors.toMap(
                        key -> (String) joinFunction.invoke(joinArgs.leftTableKeyExtractFunctionName, key),
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

//                    resultList.add(String.join(",",resultLine));
//                    resultLine.clear();
//                }
//            }
//
//        }
//        Stream<List<String>> nextStream = (Stream<List<String>>) resultList;
//        input1.setInnerResult(nextStream);

        // 左表join右表(Hash Join) (可用)
        // 哈希表的装填因子一般设为0.75
        double loadFactory = 0.75;
        int hashTableLength = 0;
        int keyHashCode;

        // 保存可以join的每一行
        List<String> resultLine = new ArrayList<>();
        // 保存所有join的行
        List<String> resultList = new ArrayList<>();

        // 确定大表和小表，小表作Build Table，大表作Probe Table
        if(rightTableMap.size() < leftTableMap.size()) {
            // 右表是小表

            // 确定哈希表表长
            hashTableLength = (int) Math.ceil(rightTableMap.size() / loadFactory);
            // 构建hashTable
            HashJoinTable hashTable = new HashJoinTable(hashTableLength);
            // 遍历rightTableHash，将其按key值构建hashTable
            for(Map.Entry<String, List<String>> entry : rightTableMap.entrySet()) {
                hashTable.put(entry.getKey(), entry.getValue());
            }
            // 遍历leftTableHash，找到hashcode相同的桶，再匹配right和left的key是否相等，相等则该项join成功
            for(Map.Entry<String, List<String>> entry : leftTableMap.entrySet()) {
                keyHashCode = hashTable.hash(entry.getKey(), hashTableLength);
                if(hashTable.nodeIsEmpty(keyHashCode)) {
                    // 如果这个hashcode对应的桶没有value，则跳过
                    continue;
                }
                // 如果该hashcode对应的桶有value
                // 则先看该索引的头节点的key值是否相等
                Node temp = hashTable.Headlist[keyHashCode];
                if(temp.key.equals(entry.getKey())) {
                    resultLine.addAll(entry.getValue());
                    resultLine.addAll((List<String>) temp.data);

                    resultList.add(String.join(",", resultLine));
                    resultLine.clear();
                } else {
                    // 再循环遍历这个hashcode索引对应的链表，比较是否存在与左表key相等的右表key
                    while(temp.next != null) {
                        temp = temp.next;
                        if(temp.key.equals(entry.getKey())) {
                            resultLine.addAll(entry.getValue());
                            resultLine.addAll((List<String>) temp.data);

                            resultList.add(String.join(",", resultLine));
                            resultLine.clear();
                        }

                    }
                }
            }

            // join结果写进输出流
            Stream<List<String>> nextStream = (Stream<List<String>>) resultList;
            input1.setInnerResult(nextStream);

        } else {
            // 左表是小表

            // 确定哈希表表长
            hashTableLength = (int) Math.ceil(leftTableMap.size() / loadFactory);
            // 构建hashTable
            HashJoinTable hashTable = new HashJoinTable(hashTableLength);
            // 遍历rightTableHash，将其按key值构建hashTable
            for(Map.Entry<String, List<String>> entry : leftTableMap.entrySet()) {
                hashTable.put(entry.getKey(), entry.getValue());
            }
            // 遍历leftTableHash，找到hashcode相同的桶，再匹配right和left的key是否相等，相等则该项join成功
            for(Map.Entry<String, List<String>> entry : rightTableMap.entrySet()) {
                keyHashCode = hashTable.hash(entry.getKey(), hashTableLength);
                if(hashTable.nodeIsEmpty(keyHashCode)) {
                    // 如果这个hashcode对应的桶没有value，则跳过
                    continue;
                }
                // 如果该hashcode对应的桶有value
                // 则先看该索引的头节点的key值是否相等
                Node temp = hashTable.Headlist[keyHashCode];
                if(temp.key.equals(entry.getKey())) {
                    resultLine.addAll(entry.getValue());
                    resultLine.addAll((List<String>) temp.data);

                    resultList.add(String.join(",", resultLine));
                    resultLine.clear();
                } else {
                    // 再循环遍历这个hashcode索引对应的链表，比较是否存在与左表key相等的右表key
                    while(temp.next != null) {
                        temp = temp.next;
                        if(temp.key.equals(entry.getKey())) {
                            resultLine.addAll(entry.getValue());
                            resultLine.addAll((List<String>) temp.data);

                            resultList.add(String.join(",", resultLine));
                            resultLine.clear();
                        }

                    }
                }
            }

            // join结果写进输出流
            Stream<List<String>> nextStream = (Stream<List<String>>) resultList;
            input1.setInnerResult(nextStream);

        }
    }

    /**
     * 构建 hash join table 需要用到的节点类
     */
    public static class Node {
        Node next;
        Object key;
        Object data;
        Integer hashCode;

        public Node(Object key, Object data, Integer hashCode) {
            this.key = key;
            this.data = data;
            this.hashCode = hashCode;
        }
    }

    /**
     * 拉链法构建 hash join table
     */
    public static class HashJoinTable {

        // 一个定长数组
        public Node[] Headlist;
        // 数组初始分配空间长度
        public int initialLength;
        // 当前哈希表元素个数
        public int size = 0;

        public HashJoinTable(int initialLength) {
            this.initialLength = initialLength;
            Headlist = new Node[initialLength];
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

        // 放置节点
        public void put(Object key, Object data) {
            int index = hash(key, Headlist.length);
            // 当前节点封装成Node节点
            Node node = new Node(key, data, index);
            // 加入哈希表
            input(node, Headlist, index);
            size++;
        }

        // 插入节点
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

        public boolean nodeIsEmpty(int index) {
            Node temp = Headlist[index];
            if(temp == null)
                return true;
            else
                return false;
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

        public void show() {
            for(int i=0;i<Headlist.length;i++) {
                if(Headlist[i] != null) {
                    System.out.print(Headlist[i].key + ":" + Headlist[i].data
                            + " hashcode is" + Headlist[i].hashCode + "-->");
                    Node temp = Headlist[i];
                    while(temp.next!=null) {
                        temp = temp.next;
                        System.out.println(temp.key + ":" + temp.data
                                + " hashcode is" + temp.hashCode + "-->");
                    }
                    //System.out.println();
                }
            }
        }

        // 该main函数仅用来测试HashJoinTable
        public static void main(String[] args) {
            //定义一定数量的键值对
            String[] key= {"sam","tom","water","banana","fine","watch","goal","new"};
            String[] data= {"1","2","3","4","5","6","7","8"};
            List<String> oneTable;
            List<String> anotherTable;

            // 测试resultLine和resultList
            List<String> resultLine = new ArrayList<>();
            List<String> resultList = new ArrayList<>();

            //初始化哈希表
            HashJoinTable table=new HashJoinTable(5);
            for(int i=0;i<key.length;i++) {
                //将每一个键值对一一加到构造好的哈希表中
                oneTable = new ArrayList<>(Arrays.asList("xxx", "yyy", "zzz"));
                table.put(key[i], oneTable);
                //table.put(key[i], data[i]);

                anotherTable = new ArrayList<>(Arrays.asList("aa", "bb", "cc"));
                resultLine.addAll((List<String>) oneTable);
                resultLine.addAll((List<String>) anotherTable);

                resultList.add(String.join(",",resultLine));
                resultLine.clear();
            }
            table.show();
            System.out.println();
            for(int i=0;i<key.length;i++) {
                //根据键值从哈希表中获取相应的数据
                Object data1= table.get(key[i]);
                System.out.print(data1 + " ");
            }

            System.out.println();
            System.out.println("-----------------resultList--------------------");
            resultList.forEach(s -> {
                System.out.println(s);
            });
        }
    }
}
