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


public class Question1{
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] remainArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (remainArgs.length != 2) {
			System.err.println("Usage: Question1 <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "Question1");
		job.setJarByClass(Question1.class);
		
		job.setMapperClass(Map.class); 
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(remainArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
		
	}
	
	public static class Map extends 
			Mapper<LongWritable,Text,NullWritable,Text>{
		private Text text = new Text();
		@Override 		
		public void map(LongWritable key,Text value,Context context)
				throws IOException, InterruptedException{
			String line = value.toString();
			String[] texts = line.split("::");
			if(Integer.parseInt(texts[2])<=7&&texts[1].equals("M")){
				text.set(texts[0]);
				context.write(NullWritable.get(),text);
			}
			
		}
		
		
	}
	
	public static class Reduce extends 
			Reducer<NullWritable,Text,NullWritable,Text>{
			private Text wr = new Text();
			public void reduce(NullWritable key,Iterable<Text> values,Context context) 
					throws IOException,InterruptedException {
				String word = null;
				for (Text val:values){
					word = val.toString();
					wr.set(word);
					context.write(NullWritable.get(),wr);
				}
				
								
	}
	}
}