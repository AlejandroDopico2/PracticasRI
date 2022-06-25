import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.util.Pair;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class SimilarDocs {
    public static String index = null;
    public static String field = null;
    public static int doc = -1;
    public static String rep = null;
    public static int top = -1;
    public static Directory directory = null;

    public static Map<String, Integer> targetDocBin = new HashMap<>();
    public static ArrayList<Pair<Map<String, Integer>, Integer>> arrayBin = new ArrayList<>();

    public static Map<String, Integer> targetDocTF = new HashMap<>();
    public static ArrayList<Pair<Map<String, Integer>, Integer>> arrayTf = new ArrayList<>();

    public static Map<String, Double> targetDocTFIDF = new HashMap<>();
    public static ArrayList<Pair<Map<String, Double>, Integer>> arrayTFIDF = new ArrayList<>();

    public static Set<String> termsSet = new HashSet<>();

    public static RealVector vectorTargetDoc = null;
    public static ArrayList<RealVector> arrayVectors = new ArrayList<>();


    private static void read_args(String[] args) {
        String usage = "java org.apache.lucene.demo.SimilarDocs"
                + " [-index INDEX_PATH] [-field SELECTED_FILE] [-doc DOC_ID] [-top N] [-rep (BIN | TF | TFXIDF)]\n\n";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    index = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-doc":
                    doc = Integer.parseInt(args[++i]);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-rep": {
                    rep = args[++i].toUpperCase();
                    if (!rep.equalsIgnoreCase("bin") && !rep.equalsIgnoreCase("tf") && !rep.equalsIgnoreCase("tfxidf")) {
                        System.err.println("Valid -rep values: bin, tf or tfxidf\nUsage" + usage);
                        System.exit(3);
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (index == null || field == null || doc == -1 || top == -1 || rep == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        Directory dir;
        read_args(args);
        try {
            dir = FSDirectory.open(Paths.get(index));
            directory = dir;
            IndexReader reader = DirectoryReader.open(directory);

            switch (rep) {
                case "BIN":
                    iterDocsBin(reader);
                    break;
                case "TF":
                    iterDocsTf(reader);
                    break;
                case "TFXIDF":
                    iterDocsTFIDF(reader);
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void iterDocsBin(IndexReader reader) throws IOException {

        Terms vectorObj = reader.getTermVector(doc, field);
        TermsEnum vectorObjIter = vectorObj.iterator();
        BytesRef targetTermName;
        ArrayList<String> arrayTermsObj = new ArrayList<>();
        while ((targetTermName = vectorObjIter.next()) != null) {
            String termObj = targetTermName.utf8ToString();
            arrayTermsObj.add(termObj);
        }

        for (int i = 0; i < reader.maxDoc(); ++i) {
            Terms vectorDocs = reader.getTermVector(i, field);
            TermsEnum vectorIter = vectorDocs.iterator();
            BytesRef term;
            Map<String, Integer> termAndFreq = new HashMap<>();

            while ((term = vectorIter.next()) != null) {
                String text = term.utf8ToString();
                    if (arrayTermsObj.contains(term.utf8ToString())) {
                        if (i != doc)
                            termAndFreq.put(text, 1);
                        else
                            targetDocBin.put(text, 1);
                    }
                    else {
                        if (i != doc)
                            termAndFreq.put(text, 0);
                        else
                            targetDocBin.put(text, 0);
                    }
                termsSet.add(text);
            }
            if (i != doc)
                arrayBin.add(new Pair<>(termAndFreq, i));
        }

        vectorTargetDoc = toRealVectorInt(targetDocBin);

        for (Pair<Map<String, Integer>, Integer> mapIntegerPair : arrayBin)
            arrayVectors.add(toRealVectorInt(mapIntegerPair.getFirst()));

        getSimilarity(top);
    }

    public static void iterDocsTf(IndexReader reader) throws IOException {
        for (int i = 0; i < reader.maxDoc(); ++i) {
            Terms vectorDocs = reader.getTermVector(i, field);
            TermsEnum vectorIter = vectorDocs.iterator();
            BytesRef term;
            Map<String, Integer> termAndFreq = new HashMap<>();

            while ((term = vectorIter.next()) != null) {
                String text = term.utf8ToString();
                int freq = (int) vectorIter.totalTermFreq();
                if (i != doc)
                    termAndFreq.put(text, freq);
                else
                    targetDocTF.put(text, freq);
                termsSet.add(text);
            }
            if (i != doc)
                arrayTf.add(new Pair<>(termAndFreq, i));
        }

        vectorTargetDoc = toRealVectorInt(targetDocTF);

        for (Pair<Map<String, Integer>, Integer> mapIntegerPair : arrayTf)
            arrayVectors.add(toRealVectorInt(mapIntegerPair.getFirst()));

        getSimilarity(top);
    }

    public static void iterDocsTFIDF(IndexReader reader) throws IOException {
        for (int i = 0; i < reader.maxDoc(); ++i) {
            Terms vectorDocs = reader.getTermVector(i, field);
            TermsEnum vectorIter = vectorDocs.iterator();
            BytesRef term;
            Map<String, Double> termAndFreq = new HashMap<>();

            while ((term = vectorIter.next()) != null) {
                String text = term.utf8ToString();
                double freq = vectorIter.totalTermFreq() * getIDF(reader, term);
                if (i != doc)
                    termAndFreq.put(text, freq);
                else
                    targetDocTFIDF.put(text, freq);
                termsSet.add(text);
            }
            if (i != doc)
                arrayTFIDF.add(new Pair<>(termAndFreq, i));
        }

        vectorTargetDoc = toRealVectorDouble(targetDocTFIDF);

        for (Pair<Map<String, Double>, Integer> mapIntegerPair : arrayTFIDF) {
            arrayVectors.add(toRealVectorDouble(mapIntegerPair.getFirst()));
        }

        getSimilarity(top);
    }

    private static double getIDF(IndexReader reader, BytesRef bytes){
        int docCount = reader.numDocs();
        int docFreq;
        double score = 0;
        try {
            docFreq = reader.docFreq(new Term(field, bytes));
            score = Math.log10((double) docCount / (double) docFreq);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new BigDecimal(score).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

   public static  RealVector toRealVectorInt(Map<String, Integer> map) {
        RealVector vector = new ArrayRealVector(termsSet.size());
        int i = 0;
        for (String term : termsSet) {
            int value = map.getOrDefault(term, 0);
            vector.setEntry(i++, value);
        }
        return vector.mapDivide(vector.getL1Norm());
    }

    public static  RealVector toRealVectorDouble(Map<String, Double> map) {
        RealVector vector = new ArrayRealVector(termsSet.size());
        int i = 0;
        for (String term : termsSet) {
            double value = map.getOrDefault(term, (double) 0);
            vector.setEntry(i++, value);
        }
        return vector.mapDivide(vector.getL1Norm());
    }

   public static void getSimilarity(int top) {

        try (Directory dir = FSDirectory.open(Paths.get(index)); IndexReader reader = DirectoryReader.open(dir)) {
            Document targetDoc = reader.document(doc);
            System.out.println("DOCUMENTO OBJETIVO: " + targetDoc.getField("path").stringValue() + "\n");

            Document similarDoc;
            Map<String, Double> pathSim = new HashMap<>();

            for (int i = 0; i < arrayVectors.size(); ++i){
                int value = 0;

                switch (rep){
                    case ("TF"):
                        value = arrayTf.get(i).getSecond();
                        break;
                    case ("BIN"):
                        value = arrayBin.get(i).getSecond();
                        break;
                    case ("TFXIDF"):
                        value = arrayTFIDF.get(i).getSecond();
                        break;
                    default:
                        System.exit(-1);
                }

                if (value != doc){
                    similarDoc = reader.document(value);
                    double sim = vectorTargetDoc.dotProduct(arrayVectors.get(i)) / (vectorTargetDoc.getNorm() * arrayVectors.get(i).getNorm());
                    if (Double.isNaN(sim))
                        sim = 0.0;
                    BigDecimal bd = new BigDecimal(sim);
                    bd = bd.setScale(3, RoundingMode.HALF_UP);
                    pathSim.put(similarDoc.getField("path").stringValue() + value, bd.doubleValue());
                }
            }

            printMap(getDoubleSorted(pathSim), top);

        } catch (IOException e) {
            e.printStackTrace();
        }
   }

    public static Object[] getDoubleSorted(Map<String, Double> sim){
        Stream<Map.Entry<String, Double>> sorted = sim.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed());
        return sorted.toArray();
    }

   public static void printMap(Object[] array, int top){
       for (int i = 0; i < array.length && i < top; ++i)
           System.out.println(i + "º\t" + array[i]);
   }
}
