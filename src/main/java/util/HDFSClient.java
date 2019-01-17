package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;


public class HDFSClient {


    private static Configuration conf;

    /**
     * 初始化
     * @return
     * @throws Exception
     */
    public static Configuration init() throws Exception {
        // 给jvm系统设置hdfs访问身份，默认是root用户 实际生产配置相应权限的用户
        System.setProperty("HADOOP_USER_NAME", "root");
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node-01:9000");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "node-01");
        conf.set("mapreduce.app-submission.cross-platform","true");
        return conf;

    }

    /**
     * 设置输出相关目录
     * @param outPath
     * @param uri
     * @param username
     * @throws Exception
     */
    public static void setOut(String outPath,String uri,String username) throws Exception{
        // 创建输出目录
        Path output = new Path(outPath);
        // 初始化hdfs客户端
        FileSystem fs = FileSystem.get(new URI(uri),conf,username);
        if(fs.exists(output)){
            fs.delete(output, true);
        }
        
    }

}
