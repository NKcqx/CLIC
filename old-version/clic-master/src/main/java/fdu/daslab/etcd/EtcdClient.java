//package fdu.daslab.etcd;
//
//import fdu.daslab.util.Configuration;
//import io.etcd.jetcd.ByteSequence;
//import io.etcd.jetcd.Client;
//import io.etcd.jetcd.KV;
//import io.etcd.jetcd.kv.GetResponse;
//import io.grpc.netty.GrpcSslContexts;
//
//import javax.net.ssl.SSLException;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.nio.charset.Charset;
//import java.util.concurrent.ExecutionException;
//
///**
// * 访问k8s的ETCD存储，存储master的一些元数据
// *
// * @author 唐志伟
// * @version 1.0
// * @since 2020/11/4 10:12 AM
// */
//@SuppressWarnings("UnstableApiUsage") // etcd  3.0库是unstable的，没办法
//public class EtcdClient {
//
//    private static KV kvClient;
//
//    static {
//        try {
//
//            Configuration configuration = new Configuration();
//            String clientUrl = configuration.getProperty("etcd-client-url");
//            File ca = new File("etcd/ca.crt"); // server证书
//            File cert = new File("etcd/key-crt.pem");  // 公钥证书
//            File key = new File("etcd/key.pem");   // 私钥
//            kvClient = Client.builder()
//                    .endpoints(clientUrl)
//                    .authority("etcd.example.com")
//                    .sslContext(GrpcSslContexts.forClient()
//                        .trustManager(ca)
//                        .keyManager(cert, key)
//                        .build())
//                    .build()
//                    .getKVClient();
//        } catch (FileNotFoundException | SSLException e) {
//            e.printStackTrace();
//        }
//    }
//
//    // 获取key对应的值
//    public static String getVal(String key) {
//        ByteSequence keySequence = ByteSequence.from(key, Charset.defaultCharset());
//        try {
//            final GetResponse response = kvClient.get(keySequence).get();
//            return response.getKvs().get(0).getValue().toString();
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//
//    public static void putVal(String key, String val) {
//        ByteSequence keySequence = ByteSequence.from(key, Charset.defaultCharset());
//        ByteSequence valSequence = ByteSequence.from(val, Charset.defaultCharset());
//        kvClient.put(keySequence, valSequence);
//    }
//
////    public static void main(String[] args) {
////        getVal("/");
////    }
//}
