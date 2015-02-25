import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import edu.mit.jwi.IDictionary;
import edu.mit.jwi.item.IIndexWord;
import edu.mit.jwi.item.ISynset;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.IWordID;
import edu.mit.jwi.item.POS;
import edu.stanford.nlp.ling.WordLemmaTag;
import edu.stanford.nlp.process.Morphology;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

public class FeaturesExtractor {

	private HashSet<String> stopwords;
	private static String[] s_stopwords = { "DT", "CD", "CC", "EX", "IN", "MD",
			"PDT", "PRP", "PRP$", "RB", "RBR", "RBS", "RP", "POS", "SYM", "TO",
			"UH", "WP", "WP$", "WRB", "WDT", "#", "$", "\"", "(", ")", ",",
			".", ":", "''", "LRB", "RRB", "LCB", "RCB", "LSB", "RSB", "-LRB-",
			"B-", "``", "FW", "-RRB-", " ", " " };

	private Morphology morphology = new Morphology();
	private MaxentTagger tagger;
	
	private String wordNetPath;

	// private Map<String, Integer> termFrequency = new HashMap<>();;

	public FeaturesExtractor(String pDocumentName, MaxentTagger tagger) {

		stopwords = new HashSet<String>(s_stopwords.length);
		for (String stopWord : s_stopwords) {
			stopwords.add(stopWord);
		}
		// this.documentName = pDocumentName;
		this.tagger = tagger;
	}
	
	public FeaturesExtractor(String pDocumentName, MaxentTagger tagger, String wordNetPath) {

		stopwords = new HashSet<String>(s_stopwords.length);
		for (String stopWord : s_stopwords) {
			stopwords.add(stopWord);
		}
		// this.documentName = pDocumentName;
		this.tagger = tagger;
		this.wordNetPath= wordNetPath;
	}

	public String getFeatures(String document) throws IOException {

		URL url = new URL("file", null, wordNetPath);
		IDictionary dict = new edu.mit.jwi.Dictionary(url);
		dict.open();

		document = tagger.tagString(document);

		String[] splittedDocument = document.split(" ");

		StringBuilder builder = new StringBuilder();

		String originalWord, pos;

		for (int i = 0; i < splittedDocument.length; i++) {

			String[] temp = splittedDocument[i].split("_");

			if(temp.length != 2)
				return null;
				
			originalWord = temp[0];
			pos = temp[1];

			if (!isStopWord(pos)) {

				originalWord = morphology.stem(originalWord);

				String stemmedWord = morphology.lemma(originalWord, pos, true);

				builder.append(stemmedWord + " ");

				// adding synonyms
				if (Constants.CONCEPTS) {
					ArrayList<String> wordSynset = getSynsets(dict,
							stemmedWord, getPos(pos));
					if (!wordSynset.isEmpty()) {
						for (String string : wordSynset)
							builder.append(string + " ");
					}
				}

			}
		}

		dict.close();
		return builder.toString();
	}

	private ArrayList<String> getSynsets(IDictionary dictionary, String word,
			POS pos) {
		ArrayList<String> allSynsets = new ArrayList<>();
		IIndexWord indexWord = dictionary.getIndexWord(word, pos);
		String synonym;
		int synsetSize;

		if (indexWord != null) {
			IWordID wordID = indexWord.getWordIDs().get(0);
			IWord iWord = dictionary.getWord(wordID);
			ISynset synset = iWord.getSynset();

			synsetSize = synset.getWords().size();
			for (int i = 0; i < synsetSize
					&& i < Constants.MAXIMUM_SYNSETS_NUMBER; i++) {
				synonym = synset.getWords().get(i).getLemma().toLowerCase();
				if (!synonym.equals(word) && synonym.length() > 3) {
					allSynsets.add(synonym);
				}
			}
		}
		return allSynsets;
	}

	private POS getPos(String pos) {
		if (pos.equals("NN") || pos.equals("NNS") || pos.equals("NNP")
				|| pos.equals("NNPS")) {
			return POS.NOUN;
		} else if (pos.equals("VB") || pos.equals("VBD") || pos.equals("VBG")
				|| pos.equals("VBN") || pos.equals("VBP") || pos.equals("VBZ")) {
			return POS.VERB;
		} else if (pos.equals("JJ") || pos.equals("JJR") || pos.equals("JJS")) {
			return POS.ADJECTIVE;
		}
		System.out.println("Equivalent POS not found! Default POS.NOUN used!");
		return POS.NOUN;
	}

	private boolean isStopWord(String aWord) {
		return stopwords.contains(aWord);
	}

}
