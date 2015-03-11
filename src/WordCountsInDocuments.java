import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class WordCountsInDocuments extends Configured implements Tool {

	private static final String OUTPUT_PATH1 = "word-frequency";
	private static final String OUTPUT_PATH2 = "word-counts";

	public static class WordCountsForDocsMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text documentName = new Text();
		private Text wordAndCount = new Text();

		// PRE-CONDITION: parola@nomeDocumento 2
		// POST-CONDITION: <"nomeDocumento", "parola=value"> pairs
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] wordCounter = value.toString().split("\t");
			String[] wordAndDocument = wordCounter[0].split("@");
			this.documentName.set(wordAndDocument[1]);
			this.wordAndCount.set(wordAndDocument[0] + "=" + wordCounter[1]);
			context.write(this.documentName, this.wordAndCount);
		}
	}

	public static class WordCountsForDocsReducer extends
			Reducer<Text, Text, Text, Text> {
		/* word@document */
		private Text wordAndDocument = new Text();
		/* n/N */
		private Text counter = new Text();

		// PRE-CONDITION <"document", [word1=3, word2=5....]
		// POST-CONDITION <word1@document, n/N>
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> frequencyInDocCounter = new HashMap<>();
			int totalWordInDocument = 0;

			for (Text value : values) {
				String[] wordCounter = value.toString().split("=");

				// frequency(n)
				frequencyInDocCounter.put(wordCounter[0],
						Integer.valueOf(wordCounter[1]));
				// total(N)
				totalWordInDocument += Integer.parseInt(value.toString().split(
						"=")[1]);
			}
			for (String word : frequencyInDocCounter.keySet()) {
				int wordFrequency = frequencyInDocCounter.get(word);

				if (!Constants.CUT_ON_FREQUENCY) {
					// no cut on threshold
					this.wordAndDocument.set(word + "@" + key.toString());
					this.counter.set(wordFrequency + "/" + totalWordInDocument);
					context.write(this.wordAndDocument, counter);

				} else {
					// cut on threshold
					int threshold = (int) Math.round(Constants.LOWER_LIMIT
							* totalWordInDocument / 100);

					if (wordFrequency >= threshold) {
						this.wordAndDocument.set(word + "@" + key.toString());
						this.counter.set(wordFrequency + "/"
								+ totalWordInDocument);
						context.write(this.wordAndDocument, counter);
					}
				}
			}
		}

	}

	public static void JobNo2Start(String[] args) throws Exception {
		ToolRunner.run(new WordCountsInDocuments(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "Documents In Corpus List");

		job.setJarByClass(this.getClass());
		job.setMapperClass(WordCountsForDocsMapper.class);
		job.setReducerClass(WordCountsForDocsReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(16);
		
		FileInputFormat.setInputPaths(job, new Path(OUTPUT_PATH1));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH2));

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
