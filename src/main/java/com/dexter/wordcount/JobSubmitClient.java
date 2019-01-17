package com.dexter.wordcount;

import Constant.SysConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.HDFSClient;

/**
 * 用于提交mapreduce job的客户端程序
 * 功能：
 * 1、封装本次job运行时所需要的必要参数
 * 2、跟yarn进行交互，将mapreduce程序成功的启动、运行
 * 1，提交 mapreduce 的job客户端代码
 */
public class JobSubmitClient {


    public static void main(String[] args) throws Exception {


        Configuration conf = HDFSClient.init();

        //初始化 job
        Job job = Job.getInstance(conf);

        // 动态获取该job所在的路径
        job.setJarByClass(JobSubmitClient.class);

        // 分别赋值mapper 和 reduce
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 本次job的Mapper实现类、Reducer实现类产生的结果数据的key、value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        HDFSClient.setOut(SysConstant.OUTDIR, "hdfs://node-01:9000", "root");

        // 封装参数：本次job要处理的输入数据集所在路径、最终结果的输出路径
        FileInputFormat.setInputPaths(job, new Path(SysConstant.INTDIR));
        // 输出路径不能存在，必须新建
        FileOutputFormat.setOutputPath(job, new Path(SysConstant.OUTDIR));

        // 封装参数：想要启动的reduce task的数量
        job.setNumReduceTasks(5);

        // 提交job给yarn
        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : -1);

    }


}
