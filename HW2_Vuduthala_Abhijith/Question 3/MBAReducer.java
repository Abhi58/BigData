

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;

public class MBAReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	public static final int MINIMUM_SUPPORT_THRESHOLD = 1 ;
	
	int minSupportCount;
	
	 protected void setup(Context context) throws IOException, InterruptedException {
		 this.minSupportCount = context.getConfiguration().getInt("minSupportCount", MINIMUM_SUPPORT_THRESHOLD);
	
	 }
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0; // total items paired
		for (IntWritable value : values) {
				sum += value.get();
		}
		
		
		if(sum < 100) // Not frequent; Discard
			return;
		System.out.println(sum);
		
		context.write(key, new IntWritable(sum));
	}
}
