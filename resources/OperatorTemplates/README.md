# OperatorMapping手册
api的接口内 如sort函数，需要能用特定OperatorTemplates创建出指定的Operator来，因此需要提供一个mapping


# Operator 配置文件（XML）手册
* ID
* name
* kind: 声明Opt.的类别，其值有:
    1. calculator: 纯计算，改变元素的值但不改变元素的数量、地址等
    2. supplier: 没有输入（或输入为数据文件路径？），仅有输出
    3. consumer: 没有输出，仅有输入
    4. transformer: 改变输入元素的数量，可能变多也可能变少（要不要分开不确定）
    5. shuffler: 不改变值、数量，仅改变位置，例如sort、shuffle等
* platforms: 底层支持的平台列表，属性有：
    * platform: 平台对象，用 *ID* 标识
        * language: 平台使用的语音
        * implementation: 平台要执行的实体(伪img概念)文件路径
        * command: img的入口命令
        * cost: 该平台提供的Opt的性能
* execute: opt.在执行时的一些动态参数
    * target_platform: 最终选择了哪个平台的implementation，用ID表示
    * input_data_path: 输入数据路径（从Channel中获取的）
    * output_data_path: 输出数据路径（从Channel中获取的）

