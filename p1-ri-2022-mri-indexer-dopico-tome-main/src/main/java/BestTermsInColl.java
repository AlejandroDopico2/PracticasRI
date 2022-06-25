import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class BestTermsInColl {
    public static String index = null;
    public static String field = null;
    public static int top = -1;
    public static boolean rev = false;
    public static Directory directory;

    private static void read_args(String[] args) {
        String usage = "java org.apache.lucene.demo.BestTermsInColl"
                + " [-index INDEX_PATH] [-field SELECTED_FIELD] [-top top] [-rev (opcional)]\n\n";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    index = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-rev":
                    rev = true;
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        if (index == null || field == null || top == -1) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        read_args(args);
        Directory dir;
        try {
            dir = FSDirectory.open(Paths.get(index));
            directory = dir;
            IndexReader reader = DirectoryReader.open(directory);
            final Terms terms = MultiTerms.getTerms(reader, field);
            final TermsEnum termsEnum = terms.iterator();

            Map<String, Integer> docFrequencies = new HashMap<>();
            Map<String, Double> idFrequencies = new HashMap<>();

            BytesRef text;

            while ((text = termsEnum.next()) != null) {
                String term = text.utf8ToString();

                int freq = termsEnum.docFreq();
                if (rev)
                    docFrequencies.put(term, freq);
                else
                    idFrequencies.put(term, getIDF(freq, reader));

            }

            if (rev) {
                System.out.println("Showing the top " + top + " terms for the field " + field + " in the index " + index + " ordered by its document frequency (DF)");
                System.out.println(BestTermsInDoc.toMapString(BestTermsInDoc.getIntegerSorted(docFrequencies), top));
            } else {
                System.out.println("Showing the top " + top + " terms for the field " + field + " in the index " + index + " ordered by its inversed document frequency (IDFlog10)");
                System.out.println(BestTermsInDoc.toMapString(BestTermsInDoc.getDoubleSorted(idFrequencies), top));
            }

            reader.close();
            dir.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static double getIDF(int docFreq, IndexReader reader) {

        double score = Math.log10((double) reader.numDocs() / (double) docFreq);

        return new BigDecimal(score).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }
}
