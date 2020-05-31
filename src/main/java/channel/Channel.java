package channel;

import basic.Operators.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @description：为{@link Operator}管理数据的存储位置、IO等工作
 */
public class Channel {
    // 暂时不把数据封装到Slot，看不到必要性

    private Operator sourceOperator; // 边的起始点
    private int output_index; // 代表 数据在source opt 的结果列表里的第output index的地方存着
    private Operator targetOperator; // 边的终点
    private int input_index; // 代表 target opt要从自己输入列表的第input index个位置拿数据

    public Channel(Operator source, int output_index, Operator target, int input_index) {
        sourceOperator = source;
        this.output_index = output_index;
        targetOperator = target;
        this.input_index = input_index;
    }

    public String getData(){
        return sourceOperator.getData(output_index);
    }

    public Operator getTargetOperator(){
        return this.targetOperator;
    }

}
