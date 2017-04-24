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

public class Question3{
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		
		String[] remainArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (remainArgs.length != 3) {
			System.err.println("Usage: Question3 <in> <out> <genre>");
			System.exit(2);
		}
		if(!remainArgs[2].equals("Fantasy")&&!remainArgs[2].equals("Action")&&!remainArgs[2].equals("Adventure")&&
				!remainArgs[2].equals("Animation")&&!remainArgs[2].equals("Children's")&&!remainArgs[2].equals("Comedy")&&
				!remainArgs[2].equals("Crime")&&!remainArgs[2].equals("Documentary")&&!remainArgs[2].equals("Drama")&&
				!remainArgs[2].equals("Film-Noir")&&!remainArgs[2].equals("Horror")&&!remainArgs[2].equals("Musical")&&
				!remainArgs[2].equals("Mystery")&&!remainArgs[2].equals("Romance")&&!remainArgs[2].equals("Sci-Fi")&&
				!remainArgs[2].equals("Thriller")&&!remainArgs[2].equals("War")&&!remainArgs[2].equals("Western")){
			System.err.println("Please input correct genres");
			System.exit(2);
		}
			
		configuration.set("genre",remainArgs[2]);
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "Question3");
		job.setJarByClass(Question3.class);
		
		job.setMapperClass(Map.class); 
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(remainArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
		
	}
	
	public static class Map extends Mapper<LongWritable,Text,NullWritable,Text>{
		private Text text = new Text();
		public void map(LongWritable key,Text value,Context context)
				throws IOException, InterruptedException{
			Configuration configuration = context.getConfiguration();
			String gen = configuration.get("genre");
			String line = value.toString();
			String[] wds = line.split("::");
			if(wds[2].contains(gen)){
				text.set(wds[1]);
				context.write(NullWritable.get(),text);
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
	
	
	
}
