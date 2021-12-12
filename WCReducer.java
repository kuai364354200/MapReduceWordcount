package com.apache.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		// 定义一个变量，用来记录单词出现的次数（频次）
		long count = 0;
		for (LongWritable val : values) {
			// 记录总次数
			/**
			 * val每次都会去values中取一个值 而此案例中的values中存储的就是一个数字1
			 */
			count = count + val.get();
//			count += val.get();
		}
		// 单词 总数
		context.write(key, new LongWritable(count));
	}

}
