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

public class movie3{
	public static class Map extends Mapper<LongWritable,Text,NullWritable,Text>{
		private Text word = new Text();
		public void map(LongWritable key,Text value,Context context)
				throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			String param = conf.get("genre");
			String line = value.toString();
			String[] words = line.split("::");
			if(words[2].contains(param)){
				word.set(words[1]);
				context.write(NullWritable.get(),word);
			}		
			
		}
	
	}
	
	public static class Reduce extends Reducer<NullWritable,Text,NullWritable,Text>{
		public void reduce(NullWritable key,Iterable<Text> values,Context context)
		throws IOException,InterruptedException {
			for(Text val:values){
				context.write(NullWritable.get(),val);
			}
		}			
	}
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: movie3 <in> <out> <genre>");
			System.exit(2);
		}
		if(!otherArgs[2].equals("Fantasy")&&!otherArgs[2].equals("Action")&&!otherArgs[2].equals("Adventure")&&
				!otherArgs[2].equals("Animation")&&!otherArgs[2].equals("Children's")&&!otherArgs[2].equals("Comedy")&&
				!otherArgs[2].equals("Crime")&&!otherArgs[2].equals("Documentary")&&!otherArgs[2].equals("Drama")&&
				!otherArgs[2].equals("Film-Noir")&&!otherArgs[2].equals("Horror")&&!otherArgs[2].equals("Musical")&&
				!otherArgs[2].equals("Mystery")&&!otherArgs[2].equals("Romance")&&!otherArgs[2].equals("Sci-Fi")&&
				!otherArgs[2].equals("Thriller")&&!otherArgs[2].equals("War")&&!otherArgs[2].equals("Western")){
			System.err.println("Please input correct genres");
			System.exit(2);
		}
			
		conf.set("genre",otherArgs[2]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "movie3");
		job.setJarByClass(movie3.class);
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
