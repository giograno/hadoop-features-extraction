public class JobController {

	public static void main(String[] args) throws Exception {

		// args: <input path, output path, number of document, no. of machines>

		WordFrequency.JobNo1Start(args);
		WordCountsInDocuments.JobNo2Start(args);
		WordInCorpusTFIDF.JobNo3Start(args);
		FinalJob.JobNo4Start(args);
		AllWordsJob.JobNo5Start(args);

	}

}
