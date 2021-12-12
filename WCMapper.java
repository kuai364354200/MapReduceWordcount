package com.apache.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// 获取一行数据
		String line = value.toString();
		// 将行数据进行切割，从数据的格式上得知，数据之间存在一个空格
		//String[] word = line.split(" ");
		//for (String string : word) {
			//System.out.print(string);
			// Text和String虽说对应的数据类型，但毕竟是不同的数据类型。
			// new Text(string)，表示将String转换为Text
		context.write(new Text(line), new LongWritable(1));
		System.out.print("\n");
		//}
	}

}
