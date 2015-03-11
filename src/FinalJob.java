import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FinalJob extends Configured implements Tool {

	private static final String OUTPUT_PATH3 = "feature-values";

	public static class FinalJobMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text documentName = new Text();
		private Text wordAndValue = new Text();

		// PRE: <key...word@doc.....value...0.322;0.3232>
		// POST: <key...doc.....value...word:0.322;0.3232>
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] wordAndFeature = value.toString().split("\t");

			String[] wordAndDocument = wordAndFeature[0].split("@");

			this.documentName.set(new Text(wordAndDocument[1]));
			this.wordAndValue.set(new Text(wordAndDocument[0] + ":"
					+ wordAndFeature[1]));
			context.write(this.documentName, this.wordAndValue);
		}
	}

	public static class FinalJobReducer extends
			Reducer<Text, Text, Text, MapWritable> {

		private MultipleOutputs<Text, MapWritable> multipleOutputs;

		private Text documentName = new Text();
		MapWritable termFrequency = new MapWritable();
		MapWritable inverseTermFrequency = new MapWritable();

		// POST: <key=doc.....value=word:0.322;0.3232>
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			documentName.set(key);

			for (Text value : values) {
				String[] wordAndValues = value.toString().split(":");
				String word = wordAndValues[0];

				String[] tfAndTfidf = wordAndValues[1].toString().split(";");

				double tfValue = Double.parseDouble(tfAndTfidf[1]);
				double tfidfValue = Double.parseDouble(tfAndTfidf[0]);

				termFrequency.put(new Text(word), new DoubleWritable(tfValue));
				inverseTermFrequency.put(new Text(word), new DoubleWritable(
						tfidfValue));
			}
			context.write(key, inverseTermFrequency);
			multipleOutputs.write(key, termFrequency, "termFrequency");
			termFrequency.clear();
			inverseTermFrequency.clear();
		}

		public void setup(Context context) {
			multipleOutputs = new MultipleOutputs<Text, MapWritable>(context);
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			multipleOutputs.close();
		}
	}

	public static void JobNo4Start(String[] args) throws Exception {
		ToolRunner.run(new FinalJob(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration configuration4 = new Configuration();
		Job job = Job.getInstance(configuration4, "Get Final Result");

		Path outputPath = new Path(arg0[1]);

		job.setJarByClass(FinalJob.class);
		job.setMapperClass(FinalJob.FinalJobMapper.class);
		job.setReducerClass(FinalJob.FinalJobReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(OUTPUT_PATH3));
		FileOutputFormat.setOutputPath(job, outputPath);

		int noMachines = Integer.parseInt(arg0[3]);
		job.setNumReduceTasks(noMachines/2);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
