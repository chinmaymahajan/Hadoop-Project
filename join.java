
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/** 
 * @author Chinmay Mahajan
 *
 */

public class join extends Configured implements Tool 
{
	public static void main(String[] args) throws Exception 
	{
		int result = ToolRunner.run(new join(), args);
		System.exit(result);
	}

	public static class Map extends Mapper<Object, Text, Text, Text> 
	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();
			String[] list = line.split(",");

			if (list.length > 4)
			{ 
				String countryCode = list[0];
				String language = list[1];
				
				if ( !(language.compareTo("English") == 1) ) 
				{
					System.out.println("> " + countryCode + " " + language);
					context.write(new Text(countryCode), new Text(""));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Iterator ite = values.iterator();
			int counter = 0;
			String actualCountry = "";
			while (ite.hasNext()) 
			{
				String line = ite.next().toString();
				if (!line.isEmpty()) 
				{
					actualCountry = line;
				}
				counter++;
			}
			
			if (counter > 1) {
				context.write(new Text(actualCountry), new Text(""));
			}
			
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(join.class);
		job.setJobName("join");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("project/country.txt"));
		FileInputFormat.addInputPath(job, new Path("project/countrylanguage.txt"));
		FileOutputFormat.setOutputPath(job, new Path("jointOutput"));

		boolean jobComplete = job.waitForCompletion(true);
			
			if(jobComplete == true)
			{
				return 0;
			}
			else
				return 1;
	}
}
