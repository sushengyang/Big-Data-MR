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




public class movie{
	public static class Map extends Mapper<LongWritable,Text,NullWritable,Text>{
		private Text word = new Text();
		@Override 		
		public void map(LongWritable key,Text value,Context context)
				throws IOException, InterruptedException{
	
			String line = value.toString();
			String[] words = line.split("::");
			if(Integer.parseInt(words[2])<=7&&words[1].equals("M")){
				word.set(words[0]);
				context.write(NullWritable.get(),word);
			}
			
		}
		
		
	}
	public static class Reduce extends 
			Reducer<NullWritable,Text,NullWritable,Text>{
			private Text res = new Text();
			public void reduce(NullWritable key,Iterable<Text> values,Context context) 
					throws IOException,InterruptedException {
				String word = null;
				for (Text val:values){
					word = val.toString();
					res.set(word);
					context.write(NullWritable.get(),res);
				}
				
								
	}
	}
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: movie <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "movie");
		job.setJarByClass(movie.class);
		job.setMapperClass(Map.class); 
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
		
	}
}