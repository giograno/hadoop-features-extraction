import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;

public class WordFrequency extends Configured implements Tool {

	private static final String OUTPUT_PATH1 = "word-frequency";
	private static final String OUTPUT_PATH2 = "word-counts";
	private static final String OUTPUT_PATH3 = "feature-values";
	private static final String OUTPUT_PATH4 = "all-words";

	public static class WordFrequencyInDocMapper extends
			Mapper<Text, BytesWritable, Text, IntWritable> {

		private Text word = new Text();
		private final IntWritable one = new IntWritable(1);
		private MaxentTagger tagger = null;
		String wordNetPath;
		String stopWord;

/**
		 * @param key
		 *            is the name of document
		 * @param value
		 *            is the content of document in bytes
		* POST-CONDITION: Output <"word@filename, [1,1,1,1]> pairs 
		*/
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			String documentName = key.toString();
			byte[] data = value.getBytes();
			InputStream inputStream = new ByteArrayInputStream(data);

			FeaturesExtractor featuresExtractor = new FeaturesExtractor(
					documentName, tagger, wordNetPath);

			String result = null;
			try {
				result = PDFExtractor.extractTextFromPDFDocument(
						new ByteArrayInputStream(data), stopWord);
			} catch (Exception e) {
				return;
			}
			inputStream.close();

			System.out.println("Extraction from : " + documentName);
			result = featuresExtractor.getFeatures(result);
			System.out.println("Finish extraction from : " + documentName);

			if (result == null) {
				return;
			}

			StringTokenizer tokenizer = new StringTokenizer(result);

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(new Text(word + "@" + documentName), one);
			}
		}

		public void setup(Context context) throws IOException {

			tagger = new MaxentTagger(
					"taggers/english-left3words-distsim.tagger");
			wordNetPath = "WordNet-3/dict";
			stopWord = "stop-word";

			System.setProperty("org.apache.pdfbox.baseParser.pushBackSize",
					"999999");
		}
	}

	public static class WordFrequencyInDocReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

/**
 * PRE-CONDITION: Input <"word@filename, [1,1,1,1]> pairs
 * POST-CONDITION: Output <"word@filename, value"> pairs
 */
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void JobNo1Start(String[] args) throws Exception {
		ToolRunner.run(new WordFrequency(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);

		Path inputPath = new Path(arg0[0]);
		Path outputPath = new Path(arg0[1]);

		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}

		Path wordFreqPath = new Path(OUTPUT_PATH1);
		if (fileSystem.exists(wordFreqPath)) {
			fileSystem.delete(wordFreqPath, true);
		}

		// Remove the phase of word counts path
		Path wordCountsPath = new Path(OUTPUT_PATH2);
		if (fileSystem.exists(wordCountsPath)) {
			fileSystem.delete(wordCountsPath, true);
		}

		Path featuresPath = new Path(OUTPUT_PATH3);
		if (fileSystem.exists(featuresPath))
			fileSystem.delete(featuresPath, true);

		Path allWordsPath = new Path(OUTPUT_PATH4);
		if (fileSystem.exists(allWordsPath))
			fileSystem.delete(allWordsPath, true);

		/* JOB No.1 */
		Job job = Job.getInstance(configuration, "Word Frequence in Document");

		job.setJarByClass(this.getClass());
		job.setMapperClass(WordFrequencyInDocMapper.class);
		job.setCombinerClass(WordFrequencyInDocReducer.class);
		job.setReducerClass(WordFrequencyInDocReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH1));

		job.setNumReduceTasks(16);
		/*
		 * Path stopWord = new Path("stop-word"); Path taggers = new
		 * Path("taggers/english-left3words-distsim.tagger"); Path wordnet = new
		 * Path("WordNet-3/dict");
		 * 
		 * job.addCacheFile(stopWord.toUri());
		 * job.addCacheFile(taggers.toUri()); job.addCacheFile(wordnet.toUri());
		 */

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
