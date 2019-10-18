

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

public class MBADriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MBADriver(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception{
		String inputPath = args[0];
		String outputPath = args[1];
		int numberOfPairs = Integer.parseInt(args[2]);
		double minimumSupportThreshold = Double.parseDouble(args[3]);
		
		int txnCount = 9684;
		int minSupportCount = (int)Math.ceil(minimumSupportThreshold*txnCount);
		
		Job job = new Job(getConf());
		
		job.getConfiguration().setInt("number.of.pairs", numberOfPairs);
		job.getConfiguration().setInt("minimum.support.count",minSupportCount);
		
		// set input/output path
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		 // output format
		job.setOutputFormatClass(TextOutputFormat.class);
		
		 // reducer K, V output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// set mapper/reducer/combiner
		job.setMapperClass(MBAMapper.class);
		job.setCombinerClass(MBAReducer.class);
		job.setReducerClass(MBAReducer.class);
		
		 //delete the output path if it exists to avoid "existing dir/file" error
		Path outputDir = new Path(outputPath);
		FileSystem.get(getConf()).delete(outputDir, true);
		
		 // submit job
		boolean status = job.waitForCompletion(true);
		
		return 0;
	}

}

