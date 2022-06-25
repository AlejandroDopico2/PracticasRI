import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

public class TrainingTestMedline {

    public static Path indexPath = null;
    public static int cut = 0;
    public static String metrica = null;
    public static int n1 = 0;
    public static int n2 = 0;
    public static int n3 = 0;
    public static int n4 = 0;
    public static boolean evaljm = false;



    private static void read_args(String[] args) {
        String usage = "java org.apache.lucene.demo.TrainingTestMedline"
                + " [-evaljm int1-int2 int3-int4] [-cut n] [-metrica P | R | MAP] [-indexin pathname]\n\n";
        String path = null;
        String[] parts1;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-indexin":
                    path = args[++i];
                    break;
                case "-cut":
                    cut = Integer.parseInt(args[++i]);
                    break;
                case "-metrica":
                    metrica = args[++i];
                    break;
                case "-evaljm":
                    evaljm = true;
                    parts1 = args[++i].split("-");
                    if (parts1.length == 2) {
                        n1 = Integer.parseInt(parts1[0]);
                        n2 = Integer.parseInt(parts1[1]);
                    }
                    String[] parts2 = args[++i].split("-");
                    if (parts2.length == 2) {
                        n3 = Integer.parseInt(parts2[0]);
                        n4 = Integer.parseInt(parts2[1]);
                    }
                    break;
                case "-evaltfidf":
                    evaljm = false;
                    parts1 = args[++i].split("-");
                    if (parts1.length == 2) {
                        n3 = Integer.parseInt(parts1[0]);
                        n4 = Integer.parseInt(parts1[1]);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        if (path == null)
            System.exit(-2);

        indexPath = Paths.get(path);
    }

    public static void main(String[] args) {
        read_args(args);

        ArrayList<Integer> trainRelevantDocsID = SearchEvalMedline.countLineBufferedReader("medline/MED.REL", n1);
        ArrayList<Integer> testRelevantDocsID = SearchEvalMedline.countLineBufferedReader("medline/MED.REL", n3);

        for (int idQuery = n1+1; idQuery <= n2; idQuery++)
            trainRelevantDocsID.addAll(SearchEvalMedline.countLineBufferedReader("medline/MED.REL", idQuery));

        for (int idQuery = n3+1; idQuery <= n4; idQuery++)
            testRelevantDocsID.addAll(SearchEvalMedline.countLineBufferedReader("medline/MED.REL", idQuery));

        if (evaljm)
            evaljm();
        else
            evaltfidf(testRelevantDocsID);


    }
    public static void evaltfidf(ArrayList<Integer> testRelevantDocsID){
        try {
            Directory dir = FSDirectory.open(indexPath);
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setSimilarity(new ClassicSimilarity());

            IndexReader reader = DirectoryReader.open(dir);
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(new ClassicSimilarity());

            BufferedReader bufferedReader = new BufferedReader(new FileReader("query/MED.QRY"));
            String contents = bufferedReader.lines().collect(Collectors.joining());
            String[] subQueries= contents.split("\\.I (\\d)+\\.W");
            QueryParser parser = new QueryParser("contents", analyzer);

            File csv = new File("medline.tfidf.training.null.test." + n3 + "-" + n4 + "."
                    + metrica + cut + ".test.csv");

            FileWriter escritor = new FileWriter(csv);
            BufferedWriter buf = new BufferedWriter(escritor);
            buf.write(", " + metrica + "@" + cut + "\n" );

            float mean = 0;
            float sum = 0;

            for (int i = n3; i <= n4; i++){
                subQueries[i] = subQueries[i].toLowerCase();
                subQueries[i] = subQueries[i].replace("(", "\"(\"");
                subQueries[i] = subQueries[i].replace(")", "\")\"");
                subQueries[i] = subQueries[i].substring(1);

                TopDocs topDocs = searcher.search(parser.parse(subQueries[i]), cut);

                int relevantRetrieved = 0;
                for (int j = 0; j < topDocs.scoreDocs.length; ++j) {
                    if (testRelevantDocsID.contains(topDocs.scoreDocs[j].doc)) {
                        relevantRetrieved++;
                        sum = (float) relevantRetrieved / j;
                    }
                }
                System.out.print("Query " + i);

                buf.write(i + ", ");
                if (metrica.equals("P")) {
                    float precision = (float) relevantRetrieved / topDocs.scoreDocs.length;
                    System.out.println(" Con precision " + metrica + "@" + cut + " = " + precision);
                    buf.write( precision + "\n");
                    mean += precision;
                }
                else if (metrica.equals("R")){
                    float recall = (float) relevantRetrieved / testRelevantDocsID.size();
                    System.out.println(" Con recall " + metrica + "@" + cut + " = " + recall);
                    buf.write(String.format(Locale.US, "%.3f",  recall)+ "\n");
                    mean += recall;

                } else if (metrica.equals("MAP")){
                    float map = sum / testRelevantDocsID.size(); // Meter calculo
                    System.out.println(" Con MAP " + metrica + "@" + cut + " = " + map);
                    buf.write(String.format(Locale.US, "%.3f",  map) + "," + "\t");
                    mean += map;
                }
            }

            buf.write("Mean, ");
            buf.write(String.valueOf(mean/(n4-n3)));

            System.out.println("Y media " + mean);

            buf.close();
            escritor.close();

        } catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void evaljm(){
        try {
            Directory dir = FSDirectory.open(indexPath);
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            IndexReader reader = DirectoryReader.open(dir);
            IndexSearcher searcher = new IndexSearcher(reader);

            BufferedReader bufferedReader = new BufferedReader(new FileReader("query/MED.QRY"));
            String contents = bufferedReader.lines().collect(Collectors.joining());
            String[] subQueries= contents.split("\\.I (\\d)+\\.W");
            QueryParser parser;

            File csv = new File("medline.jm.training." + n1 + "-" + n2 + ".test." + n3 + "-" + n4 + "."
                    + metrica + cut + ".training.csv");
            FileWriter escritor = new FileWriter(csv);
            BufferedWriter buf = new BufferedWriter(escritor);
            buf.write(metrica + "@" + cut + ", ");
            for (BigDecimal lambda2 = new BigDecimal("0.1"); lambda2.floatValue() <= 1; lambda2 = lambda2.add( new BigDecimal("0.1"))) {
                buf.write(lambda2 + "," + "\t");
            }
            buf.write("\n");

            float[] promedios = new float[10];

            ArrayList<Integer> trainRelevantDocsID;
            for (int i = n1; i <= n2; i++) {

                trainRelevantDocsID = SearchEvalMedline.countLineBufferedReader("medline/MED.REL", i);

                buf.write(i + ", ");
                subQueries[i] = subQueries[i].toLowerCase();
                subQueries[i] = subQueries[i].replace("(", "\"(\"");
                subQueries[i] = subQueries[i].replace(")", "\")\"");
                subQueries[i] = subQueries[i].substring(1);
                int j = 0;
                float sum = 0;
                for (float lambda = 0.1f ; lambda <= 1.1; lambda += 0.1) {
                    if (lambda > 1)
                        lambda = 1;

                    iwc.setSimilarity(new LMJelinekMercerSimilarity(lambda));
                    searcher.setSimilarity(new LMJelinekMercerSimilarity(lambda));
                    parser = new QueryParser("contents", analyzer);

                    TopDocs topDocs = searcher.search(parser.parse(subQueries[i]), cut);

                    int relevantRetrieved = 0;
                    for (ScoreDoc doc : topDocs.scoreDocs){
                        if (trainRelevantDocsID.contains(doc.doc)){
                            relevantRetrieved++;
                            sum = (float) relevantRetrieved / j;
                        }
                    }
                    System.out.print("Query " + i + " Lambda " + lambda);

                    if (metrica.equals("P")) {
                        float precision = (float) relevantRetrieved / topDocs.scoreDocs.length;
                        System.out.println(" Con precision " + metrica + "@" + cut + " = " + precision);
                        buf.write( precision + "," + "\t");
                        precision += promedios[j];
                        promedios[j] = precision;
                    }
                    else if (metrica.equals("R")){
                        float recall = (float) relevantRetrieved / trainRelevantDocsID.size();
                        System.out.println(" Con recall " + metrica + "@" + cut + " = " + recall);
                        buf.write(String.format(Locale.US, "%.3f",  recall) + "," + "\t");
                        recall += promedios[j];
                        promedios[j] = recall;
                    } else if (metrica.equals("MAP")){
                        float map = sum / trainRelevantDocsID.size(); // Meter calculo
                        System.out.println(" Con MAP " + metrica + "@" + cut + " = " + map);
                        buf.write(String.format(Locale.US, "%.3f",  map) + "," + "\t");
                        map += promedios[j];
                        promedios[j] = map;
                    }
                    j++;
                }
                buf.write("\n");
            }
            buf.write("Mean, ");
            System.out.print("Media de ");
            for (float valor : promedios){
                buf.write(valor/(n2-n1) + ", ");
                System.out.print(valor/(n2-n1) + ", ");
            }
            buf.close();
            escritor.close();


            // Ahora se procede al test
            float lambda = getIndexOfLargest(promedios);
            lambda = (lambda + 1 )/10;
            searcher.setSimilarity(new LMJelinekMercerSimilarity(lambda));
            parser = new QueryParser("contents", analyzer);

            File testCSV = new File("medline.jm.training." + n1 + "-" + n2 + ".test." + n3 + "-" + n4 + "."
                    + metrica + cut + ".test.csv");
            FileWriter escritor2 = new FileWriter(testCSV);
            BufferedWriter buf2 = new BufferedWriter(escritor2);

            buf2.write(lambda + ", " + metrica + "@" + cut + "\n");

            float mean = 0;
            float sum = 0;
            ArrayList<Integer> testRelevantDocsID;
            for (int i = n3; i <= n4; i++){

                testRelevantDocsID = SearchEvalMedline.countLineBufferedReader("medline/MED.REL", i);

                subQueries[i] = subQueries[i].toLowerCase();
                subQueries[i] = subQueries[i].replace("(", "\"(\"");
                subQueries[i] = subQueries[i].replace(")", "\")\"");
                subQueries[i] = subQueries[i].substring(1);

                TopDocs topDocs = searcher.search(parser.parse(subQueries[i]), cut);

                int relevantRetrieved = 0;
                for (int j = 0; j < topDocs.scoreDocs.length; ++j) {
                    if (testRelevantDocsID.contains(topDocs.scoreDocs[j].doc)) {
                        relevantRetrieved++;
                        sum = (float) relevantRetrieved / j;
                    }
                }
                buf2.write(i + ", ");
                if (metrica.equals("P")) {
                    float precision = (float) relevantRetrieved / topDocs.scoreDocs.length;
                    buf2.write( precision + "\n");
                    mean += precision;
                }
                else if (metrica.equals("R")){
                    float recall = (float) relevantRetrieved / testRelevantDocsID.size();
                    buf2.write(String.format(Locale.US, "%.3f",  recall)+ "\n");
                    mean += recall;
                } else if (metrica.equals("MAP")){
                    float map = sum / testRelevantDocsID.size(); // Meter calculo
                    System.out.println(" Con MAP " + metrica + "@" + cut + " = " + map);
                    buf2.write(String.format(Locale.US, "%.3f",  map) + "," + "\t");
                }
            }

            buf2.write("Mean, ");
            buf2.write(String.valueOf(mean/(n4-n3)));


            buf2.close();
            escritor2.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static int getIndexOfLargest(float[] array)
    {
        if ( array == null || array.length == 0 ) return -1; // null or empty

        int largest = 0;
        for ( int i = 1; i < array.length; i++ )
        {
            if ( array[i] > array[largest] ) largest = i;
        }
        return largest; // position of the first largest found
    }

}