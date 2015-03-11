import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

public class WordInCorpusTFIDF extends Configured implements Tool {

	private static final String OUTPUT_PATH2 = "word-counts";
	private static final String OUTPUT_PATH3 = "feature-values";

	public static class WordInCorpusTFIDFMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text docAndCount = new Text();

		// PRE-CONDITION: <parola@documento n/N>
		// POST-CONDITION: <parola [document=n/N]....[]>
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] wordAndCounters = value.toString().split("\t");
			String[] wordAndDoc = wordAndCounters[0].split("@");
			this.word.set(new Text(wordAndDoc[0]));
			this.docAndCount.set(wordAndDoc[1] + "=" + wordAndCounters[1]);
			context.write(this.word, this.docAndCount);
		}
	}

	public static class WordInCorpusTFIDFReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text wordAndDocument = new Text();
		private Text tfidfInfo = new Text();

		// PRE-CONDITION: <word, [doc1=n/N, doc2=n/N]>
		// POST-CONDITION: <word@doc, [TFIDF=0.11;TF=0.2322]>
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long documentInCorpus = context.getConfiguration().getLong(
					"documentInCorpus", 0);
			int numberOfDocsWhereKeyAppears = 0;

			Set<Text> wordOutput = new HashSet<>();
			wordOutput.add(key);

			Map<String, String> frequencies = new HashMap<>();

			for (Text value : values) {
				String[] documentAndFrequencies = value.toString().split("=");
				// check for > 0
				if (Integer.parseInt(documentAndFrequencies[1].split("/")[0]) > 0) {
					numberOfDocsWhereKeyAppears++;
				}
				// key: document value: n/N
				frequencies.put(documentAndFrequencies[0],
						documentAndFrequencies[1]);
			}

			for (String document : frequencies.keySet()) {
				String[] nAndN = frequencies.get(document).split("/");

				double tf = Double.valueOf(Double.valueOf(nAndN[0])
						/ Double.valueOf(nAndN[1]));

				// document in corpus da 0
				double idf = Math
						.log10((double) documentInCorpus
								/ (double) ((numberOfDocsWhereKeyAppears == 0 ? 1
										: 0) + numberOfDocsWhereKeyAppears));

				double tfidf = tf * idf;

				this.wordAndDocument.set(key + "@" + document);
				this.tfidfInfo.set(tfidf + ";" + tf);
				context.write(this.wordAndDocument, tfidfInfo);
			}
		}
	}

	public static void JobNo3Start(String[] args) throws Exception {
		ToolRunner.run(new WordInCorpusTFIDF(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {

		long documentInCorpus = Long.parseLong(arg0[2]);

		Configuration configuration = new Configuration();
		configuration.setLong("documentInCorpus", documentInCorpus);
		Job job = Job.getInstance(configuration, "Number Of Docs In Corpus");

		job.setJarByClass(WordInCorpusTFIDF.class);
		job.setMapperClass(WordInCorpusTFIDF.WordInCorpusTFIDFMapper.class);
		job.setReducerClass(WordInCorpusTFIDF.WordInCorpusTFIDFReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		int noMachines = Integer.parseInt(arg0[3]);
		job.setNumReduceTasks(noMachines/2);
		
		FileInputFormat.setInputPaths(job, new Path(OUTPUT_PATH2));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH3));

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
