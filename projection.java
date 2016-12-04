import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** 
 * @author Chinmay Mahajan
 */

public class projection extends Configured implements Tool 
{
	public static void main(String[] args) throws Exception 
	{
		int result = ToolRunner.run(new projection(), args);
		System.exit(result);
	}

	public static class Map extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{	
		String line = value.toString();
		String[] list = line.split(",");
	
			if (list.length == 5) 
			{			
				 String city = list[1]; //city
				 String district = list[3]; //district
					context.write(new Text(city), new Text(district));
			}
			else 
			{
				System.out.println("Error!!");
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
			{
				context.write(new Text(key), new Text(""));
			}
		}
	
	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf());
		job.setJarByClass(projection.class);
		job.setJobName("projection");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("project/city.txt"));
		FileOutputFormat.setOutputPath(job, new Path("projectionOutput"));
		
		boolean jobComplete = job.waitForCompletion(true);
			
			if(jobComplete == true)
			{
				return 0;
			}
			else
				return 1;
	}
}
