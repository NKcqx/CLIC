package fdu.daslab.common.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * 定义一个用于日志的切片，打印所有的service的服务调用的输入输出
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 8:20 PM
 */
@Aspect
@Component
public class LogAspect {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // 定义切点Pointcut，表示所有的service包下面的所有handler方法
    @Pointcut("execution(* fdu.daslab.*.service.*.*(..))")
    public void executeHandler() {
    }

    // 执行之前打印参数，执行之后打印结果
    @Around("executeHandler()")
    public Object printParamAndReturning(ProceedingJoinPoint joinPoint) throws Throwable {
        String prefix = joinPoint.getTarget().getClass().getName() + "." +
                joinPoint.getSignature().getName();
        logger.info(prefix + ":请求参数:" + Arrays.toString(joinPoint.getArgs()));

        Object result = joinPoint.proceed();
        logger.info(prefix + ":返回值:" + result);
        return result;
    }
}
