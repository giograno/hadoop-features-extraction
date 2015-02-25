import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class AllWordsJob extends Configured implements Tool {

	private static final String OUTPUT_PATH2 = "word-counts";
	private static final String OUTPUT_PATH4 = "all-words";

	public static class AllWordsJobMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private Text word = new Text();

		// PRE-CONDITION: <parola@documento n/N>
		// POST-CONDITION: <parola nullWritable>
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] wordAndCounters = value.toString().split("\t");
			String[] wordAndDoc = wordAndCounters[0].split("@");
			this.word.set(new Text(wordAndDoc[0]));
			context.write(this.word, NullWritable.get());
		}
	}

	public static class AllWordsJobReduce extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> value,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void JobNo5Start(String[] args) throws Exception {
		ToolRunner.run(new AllWordsJob(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration configuration5 = new Configuration();
		Job job5 = Job.getInstance(configuration5, "All words in Documents");

		job5.setJarByClass(AllWordsJob.class);
		job5.setMapperClass(AllWordsJobMapper.class);
		job5.setReducerClass(AllWordsJobReduce.class);

		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(NullWritable.class);

		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(NullWritable.class);

		job5.setInputFormatClass(TextInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job5, new Path(OUTPUT_PATH2));
		FileOutputFormat.setOutputPath(job5, new Path(OUTPUT_PATH4));

		return job5.waitForCompletion(true) ? 0 : 1;
	}

}
