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



public class Question2{
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] remainArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		if (remainArgs.length != 2) {
			System.err.println("Usage: Question2 <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "Question2");
		job.setJarByClass(Question2.class);
		
		job.setMapperClass(Map.class); 
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(remainArgs[0])); 
		FileOutputFormat.setOutputPath(job, new Path(remainArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class Map extends 
				Mapper<LongWritable,Text,Text,IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text text = new Text();
		public void map(LongWritable key,Text value,Context context)
				throws IOException, InterruptedException{
			String line = value.toString();
			String[] lines = line.split("::");
			text.set(lines[2]+"\t"+lines[1]);
			context.write(text,one);
			
		}
	}
	
	public static class Reduce extends 
				Reducer<Text,IntWritable,Text,IntWritable>{
				private static IntWritable wr = new IntWritable();
				public void reduce(Text key,Iterable<IntWritable> values,Context context)
						throws IOException, InterruptedException {
					int sum = 0;
					for(IntWritable value:values){
						sum += value.get();
					}
					wr.set(sum);
					context.write(key,wr);
				}
	}
	
}