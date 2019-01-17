package com.dexter.hdfs.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;


@Slf4j
public class HDFSClient {

    //设置成员变量
    private static FileSystem fs;
    private static Configuration conf;

    //初始化FileSystem
    @PostConstruct
    public void init() throws Exception {
        // 从classpath中读取core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml等文件
        conf = new Configuration();
        // 保存的副本数量
        conf.set("dfs.replication", "3");
        // 上传文件的切片大小
        conf.set("dfs.blocksize", "512m");
        // 创建hdfs客户端 设置uri conf hdfs用户名
        fs = FileSystem.get(new URI("hdfs://node-01:9000/"), conf, "root");

    }


    public static void main(String[] args) throws Exception {
        downLoadFile();
        mvFile();
        mkdir();
        queryFile();
        queryFileDetail();
        readData();
        testWriteData();
        readRandomData();
        // 关闭客户端
        fs.close();
    }

    /**
     * 在hdfs中创建文件夹
     */
    public static void mkdir() throws Exception {

        fs.mkdirs(new Path("/a/b/c"));

    }


    /**
     * 内部移动文件\修改名称
     * @throws Exception
     */
    private static void mvFile() throws Exception {
        fs.rename(new Path("/a.log"),
                new Path("/tmp/b.log"));
    }

    /**
     * 从HDFS中下载文件
     * @throws IOException
     */
    public static void downLoadFile() throws IOException {
        fs.copyToLocalFile(new Path("/filename.txt"), new Path("d:/"));
    }
    /**
     * 查询hdfs指定目录下的文件信息
     */
    public static void queryFile() throws Exception {

        // 只查询文件的信息,不返回文件夹的信息
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);

        while (iterator.hasNext()) {
            LocatedFileStatus status = iterator.next();
            log.info("--------------------------------");
            log.info("文件全路径：{}", status.getPath());
            log.info("块大小：{}", status.getBlockSize());
            log.info("文件长度：{}", status.getLen());
            log.info("副本数量：{}", status.getReplication());
            log.info("块信息：{}",  Arrays.toString(status.getBlockLocations()));
            log.info("--------------------------------");
        }
    }

    /**
     * 查询hdfs指定目录下的文件和文件夹信息
     */
    public static void queryFileDetail() throws Exception {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status : listStatus) {

            log.info("--------------------------------");
            log.info("文件全路径：{}", status.getPath());
            log.info(status.isDirectory() ? "这是文件夹" : "这是文件");
            log.info("块大小：{}", status.getBlockSize());
            log.info("--------------------------------");
        }
    }

    /**
     * 读取hdfs中的文件的内容
     */
    public static void readData() throws IllegalArgumentException, IOException {

        FSDataInputStream in = fs.open(new Path("/test.txt"));

        BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));

        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        br.close();
        in.close();

    }

    /**
     * 读取hdfs中文件的指定偏移量范围的内容
     */
    public static void readRandomData() throws IllegalArgumentException, IOException {

        FSDataInputStream in = fs.open(new Path("/test.dat"));

        // 将读取的起始位置进行指定
        in.seek(2);

        // 读16个字节
        byte[] buf = new byte[64];
        in.read(buf);

        log.info("读取内容：",new String(buf));

        in.close();
        fs.close();

    }

    /**
     * 往hdfs中的文件写内容
     */

    public static void testWriteData() throws IllegalArgumentException, IOException {

        FSDataOutputStream out = fs.create(new Path("/tmp.jpg"), false);

        //C:/Users/dexter/Desktop

        FileInputStream in = new FileInputStream("C:/Users/dexter/Desktop/IMG_0157.JPG");

        byte[] buf = new byte[1024];
        int read = 0;
        while ((read = in.read(buf)) != -1) {
            out.write(buf,0,read);
        }

        in.close();
        out.close();

    }


}
