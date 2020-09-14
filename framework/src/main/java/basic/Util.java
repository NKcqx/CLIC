package basic;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/14 9:59 上午
 */
public class Util {
    public static Supplier<String> IDSupplier = new Supplier<String>() {
        @Override
        public String get() {
            return String.valueOf(UUID.randomUUID().hashCode());
        }
    };
}
