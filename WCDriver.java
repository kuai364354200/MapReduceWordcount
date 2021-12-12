package com.apache.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver {

	public static void main(String[] args) throws Exception {

		// 1. 获取配置文件
		Configuration conf = new Configuration();

		// 2. 获取代表当前mr作业的job任务对象
		Job job = Job.getInstance(conf);

		// 3. 设置当前程序的入口
		job.setJarByClass(WCDriver.class);

		// 4. 指定当前mapper任务的类
		job.setMapperClass(WCMapper.class);

		// 5. 指定当前的Reducer任务的类
		job.setReducerClass(WCReducer.class);

		/**
		 * 6. 配置Mapper的结果类型
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		/**
		 * 7. Reduce的结果类型 Reduce阶段的结果类型是直接output，不像Map阶段的输出
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		/**
		 * 8配置路径： 8.1 配置文件的输入路径 8.2 配置文件的输出路径
		 */

		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.192.131:9000/mr_word/ip.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.192.131:9000/mr_word/result"));

		// 9. 提交任务
		job.waitForCompletion(true);
	}
}
