/*****************************************************************************
*  底层平台上抽象定义的算子，各个平台之间通用
*
*  @author   xxm
*  @version  1.0
*
*****************************************************************************/
#ifndef EXECUTION_OPERATOR_HPP
#define EXECUTION_OPERATOR_HPP

namespace clic {
    class ExecutionOperator {
        public:
            virtual void execute() = 0;
            virtual ~ExecutionOperator(){};
    };
}

#endif