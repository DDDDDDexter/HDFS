package com.dexter.hdfs.wordcount;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


@Slf4j
public class HdfsWordcount {

    public static void main(String[] args) throws Exception{


        Path input = new Path("/wordcount/input");
        Path output = new Path("/wordcount/output");
        //反射获取类
        Class<?> mapperImpl = Class.forName("com.dexter.hdfs.wordcount.MapperImpl");
        Mapper mapper = (Mapper) mapperImpl.newInstance();

        Context context  =  new Context();

        /**
         * 处理数据
         */

        FileSystem fs = FileSystem.get(new URI("hdfs://node-01:9000"), new Configuration(), "root");
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(input, false);

        while(iter.hasNext()){
            // 逐行读取文件
            LocatedFileStatus file = iter.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = br.readLine()) != null) {
                //处理数据
                mapper.dealMap(line, context);
                log.debug("处理后文件内容",context);
            }

            br.close();
            in.close();

        }



        /**
         * 输出结果
         */
        HashMap<Object, Object> contextMap = context.getContextMap();

        FSDataOutputStream out = fs.create(new Path(output,new Path("out.dat")));

        Set<Map.Entry<Object, Object>> entrySet = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            out.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n")
                    .getBytes());
        }

        out.close();

        fs.close();

        log.debug("数据输出完成");

    }
}
