import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

public class PDFExtractor {
	// private static final String STOP_WORDS = "stop-word";

	public static String extractTextFromPDFDocument(InputStream pDocument,
			String stopWord) throws Exception {
		PDFTextStripper pdfTextStripper = null;
		PDDocument pdDocument = null;
		String extractText = null;

		PDFParser parser = new PDFParser(pDocument);
		parser.parse();
		pdDocument = parser.getPDDocument();
		pdfTextStripper = new PDFTextStripper();
		extractText = pdfTextStripper.getText(pdDocument);
		pdDocument.close();

		return processingText(extractText, stopWord);
	}

	private static String processingText(String document, String stopWord)
			throws Exception {
		StringBuilder sBuilder = new StringBuilder();

		List<String> stopWords = FileUtils.readLines(new File(stopWord));
		CharArraySet stopWSet = new CharArraySet(stopWords, true);

		TokenStream tokenStream = new LetterTokenizer(new StringReader(
				document.trim()));
		tokenStream = new StopFilter(tokenStream, stopWSet);

		CharTermAttribute charTermAttribute = tokenStream
				.addAttribute(CharTermAttribute.class);

		tokenStream.reset();
		while (tokenStream.incrementToken()) {
			String string = charTermAttribute.toString();
			if (Utils.isAscii(string) && string.length() > 3) {
				sBuilder.append(string + " ");
			}
		}
		tokenStream.close();

		return sBuilder.toString();
	}
}
