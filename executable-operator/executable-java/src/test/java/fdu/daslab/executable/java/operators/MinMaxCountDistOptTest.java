package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.utils.ReflectUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/28 11:00
 */
public class MinMaxCountDistOptTest {
    String udfPath ="TestNewOperatorFunc.class";

    List<String> a = Arrays.asList("www.baidu.com,5".split(","));
    List<String> b = Arrays.asList("www.sina.com,1".split(","));
    List<String> c = Arrays.asList("www.google.com,10".split(","));
    List<String> d = Arrays.asList("www.cctv.com,8".split(","));
    List<String> e = Arrays.asList("www.cctv.com,8".split(","));
    Stream<List<String>> data = Arrays.asList(a,b,c,d,e).stream();
    Stream<List<String>> minData=Arrays.asList(b).stream();
    Stream<List<String>> maxData=Arrays.asList(c).stream();
    Stream<List<String>> distData = Arrays.asList(a,b,c,d).stream();
    Stream<Long> myCount = Stream.of((long)5);

    FunctionModel functionModel = ReflectUtil.createInstanceAndMethodByPath(udfPath);
    ParamsModel inputArgs = new ParamsModel(functionModel);

    @Test
    public void minOperatorTest(){
        Map<String,String> params = new HashMap<String, String>();
        params.put("udfName","minFunc");

        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        MinOperator minOperator = new MinOperator("1",in,out, params);
        minOperator.setInputData("data",data);
        minOperator.execute(inputArgs,null);
        Stream<List<String>> res=minOperator.getOutputData("result");

        Assert.assertArrayEquals(res.toArray(), minData.toArray());
    }
    @Test
    public void maxOperatorTest(){
        Map<String,String> params = new HashMap<String, String>();
        params.put("udfName","maxFunc");

        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        MaxOperator maxOperator = new MaxOperator("1",in,out, params);
        maxOperator.setInputData("data",data);
        maxOperator.execute(inputArgs,null);
        Stream<List<String>> res = maxOperator.getOutputData("result");

        Assert.assertArrayEquals(res.toArray(), maxData.toArray());

    }
    @Test
    public void countOperatorTest(){
        Map<String,String> params = new HashMap<String, String>();

        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        CountOperator countOperator = new CountOperator("1",in,out, params);
        countOperator.setInputData("data",data);
        countOperator.execute(inputArgs,null);
        Stream<Long> res=countOperator.getOutputData("result");

        Assert.assertArrayEquals(res.toArray(), myCount.toArray());

    }
    @Test
    public void distinctOperatorTest(){
        Map<String,String> params = new HashMap<String, String>();

        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        DistinctOperator distinctOperator = new DistinctOperator("1",in,out, params);
        distinctOperator.setInputData("data",data);
        distinctOperator.execute(inputArgs,null);
        Stream<List<String>> res=distinctOperator.getOutputData("result");

        Assert.assertArrayEquals(res.toArray(), distData.toArray());

    }


}
