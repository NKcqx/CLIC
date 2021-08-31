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