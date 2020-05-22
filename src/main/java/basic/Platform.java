package basic;

public abstract class Platform {

    /**
     * 提供将抽象的Opt映射为平台自己的 Executable Opt
     * @param origin 抽象Opt的Class
     * @return Executable Opt的Class
     */
    public abstract String mappingOperator(String origin);
}
