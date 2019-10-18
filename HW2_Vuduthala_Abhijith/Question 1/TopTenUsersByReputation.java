
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopTenUsersByReputation extends Configured implements Tool {

	public static class TopTenMapper extends
			Mapper<Object, Text, IntWritable, TextArrayWritable> {
		private TreeMap<Integer, Pair<String, String, String>> repToRecordMap = new TreeMap<Integer, Pair<String, String, String>>();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = utils.transformXmlToMap(value.toString());

			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");
			String displayName = parsed.get("DisplayName");
			String location = parsed.get("Location");
			String flag  = "united states";

			if (utils.isNullOrEmpty(userId) || utils.isNullOrEmpty(reputation)
					|| !utils.isInteger(reputation) || utils.isNullOrEmpty(displayName)|| utils.isNullOrEmpty(location)) {
				return;
			}

			// of course in ties, last one in wins
			int rep = Integer.parseInt(reputation);
			if(flag.equals(location.trim().toLowerCase())) {
				//System.out.println("Coming here "+ location);
				repToRecordMap.put(rep, new Pair<String, String, String>(userId, displayName, location));
			}
			
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (int key : repToRecordMap.keySet()) {
				Pair<String, String, String> pair = repToRecordMap.get(key);
				context.write(new IntWritable(key),
						TextArrayWritable.from(pair));
			}
		}
	}

	/**
	 * In the book example there is no cleanup because a NullWritable is used as
	 * the key, so reduce only happens once. Because I'm using an IntWritable
	 * for the key I needed to output the tree during cleanup, just like the
	 * mapper.
	 * 
	 * @author geftimoff
	 * 
	 */
	public static class TopTenReducer
			extends
			Reducer<IntWritable, TextArrayWritable, IntWritable, TextArrayWritable> {
		private TreeMap<Integer, Pair<Writable, Writable, Writable>> repToRecordMap = new TreeMap<Integer, Pair<Writable, Writable, Writable>>();

		@Override
		public void reduce(IntWritable key, Iterable<TextArrayWritable> values,
				Context context) throws IOException, InterruptedException {
			for (ArrayWritable value : values) {
				
				repToRecordMap.put(Integer.valueOf(key.get()),new Pair<Writable, Writable, Writable>(value.get()[0], value.get()[1], value.get()[2]));
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (int reputation : repToRecordMap.descendingKeySet()) {
				Pair<Writable, Writable, Writable> pair = repToRecordMap.get(reputation);
				context.write(new IntWritable(reputation),
						new TextArrayWritable(pair));
			}
		}
	}

	public static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable() {
			super(Text.class);
		}

		public TextArrayWritable(Pair<Writable, Writable, Writable> pair) {
			super(Text.class, new Text[] { (Text) pair.getFirst(),
					(Text) pair.getSecond(), (Text) pair.getThird() });
		}

		public static TextArrayWritable from(Pair<String, String, String> pair) {
			Pair<Writable, Writable, Writable> p = new Pair<Writable, Writable, Writable>(new Text(
					pair.getFirst()), new Text(pair.getSecond()), new Text(pair.getThird()));
			return new TextArrayWritable(p);
		}

		@Override
		public String toString() {
			Writable[] writables = this.get();
			if (writables == null || writables.length == 0) {
				return "";
			}
			StringBuilder sb = new StringBuilder(10 * writables.length);
			for (Writable w : this.get()) {
				sb.append(w.toString());
				sb.append('\t');
			}
			return sb.toString().trim();
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new TopTenUsersByReputation(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopTenUsersByReputation <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		Job job = new Job(conf, "Top Ten Users by Reputation");
		job.setJarByClass(TopTenUsersByReputation.class);
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(TextArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}
}
