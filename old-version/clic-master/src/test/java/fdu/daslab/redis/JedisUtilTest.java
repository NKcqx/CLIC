//package fdu.daslab.redis;
//
//import fdu.daslab.scheduler.model.KubernetesStage;
//import org.junit.Test;
//import redis.clients.jedis.Jedis;
//
//
///**
// * @author 唐志伟
// * @version 1.0
// * @since 2020/11/13 2:32 PM
// */
//public class JedisUtilTest {
//
//    @Test
//    public void testConnect() {
//        final Jedis jedis = JedisUtil.getClient();
//        assert jedis != null;
//        assert jedis.get("tzw").equals("handsome");
//        jedis.close();
//    }
//
//    @Test
//    public void testCreateStage(){
//        KubernetesStage stage = new KubernetesStage("testStageName");
//        StageRepository stageRepository = new StageRepository();
//        stageRepository.addOrUpdateStage(stage);
//    }
//
//}
