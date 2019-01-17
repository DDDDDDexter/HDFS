package com.dexter.wordcount;

import bean.FlowBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 流程统计的mapper阶段代码
 */
@Slf4j
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {

		// 获取第一行代码
		String line = value.toString();
		// 根据分隔符切分日志文件
		String[] words = line.split(" ");

		// 拿到我们需要的字段
		String userId=words[1];
		long u_flow=Long.parseLong(words[88]);
		long d_flow=Long.parseLong(words[666]);

		//以kv形式输出
		context.write(new Text(userId), new FlowBean(userId,u_flow,d_flow));

	}
}
