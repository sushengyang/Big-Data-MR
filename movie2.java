import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class movie2{
	public static class Map extends 
				Mapper<LongWritable,Text,Text,IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key,Text value,Context context)
				throws IOException, InterruptedException{
			String line = value.toString();
			String[] lines = line.split("::");
			word.set(lines[2]+"\t"+lines[1]);
			context.write(word,one);
			
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
				private static IntWritable res = new IntWritable();
				public void reduce(Text key,Iterable<IntWritable> values,Context context)
						throws IOException, InterruptedException {
					int sum = 0;
					for(IntWritable value:values){
						sum += value.get();
					}
					res.set(sum);
					context.write(key,res); // context.write(key, L)
				}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: movie2 <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "movie2");
		job.setJarByClass(movie2.class);
		job.setMapperClass(Map.class); 
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
}