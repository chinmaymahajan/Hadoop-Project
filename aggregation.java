import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/**
 * @author Chinmay Mahajan
 */

public class aggregation extends Configured implements Tool 
{	
		public static void main(String[] args) throws Exception 
		{
			int result = ToolRunner.run(new aggregation(), args);
			System.exit(result);
	    }

	public static class Map extends Mapper<Object, Text, Text, IntWritable> 
	{
		
		private IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] list = line.split(",");
			String district = list[3];
			context.write(new Text(district), one);
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
	{

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			IntWritable sum = new IntWritable(0);
			for (IntWritable val:values) 
			{
				sum = new IntWritable(val.get() + sum.get());
			}
			context.write(key, sum);
		}
	}

	@Override
	public int run(String[] args) throws Exception 
	{

		Job job = new Job(getConf());
		job.setJarByClass(aggregation.class);
		job.setJobName("aggregation");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("project/city.txt"));
		FileOutputFormat.setOutputPath(job, new Path("aggregationOutput"));

		boolean jobComplete = job.waitForCompletion(true);

			if(jobComplete == true)
			{
				return 0;
			}
			else
				return 1;
	}
}