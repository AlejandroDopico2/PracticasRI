import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class BestTermsInDoc {

    public static Path indexPath = null;
    public static int top = -1;
    public static int docId = -1;
    public static String fieldName = null;
    public static String order = null;
    public static String outputFilePath = null;

    public static Map<String, Integer> termFrequencies = new HashMap<>();
    public static Map<String, Integer> docFrequencies = new HashMap<>();
    public static Map<String, Double> idFrequencies = new HashMap<>();
    public static Map<String, Double> tf_idfFrequencies = new HashMap<>();


    private static void read_args(String[] args) {
        String usage = "java WriteIndex [-index INDEX_PATH] [-docID DOC_ID] [-field FIELD_NAME] [-top TOP] [-order (TF, DF, IDF, TFXIDF)] [-outputfile OUTPUTFILE_PATH]";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = Paths.get(args[++i]);
                    break;
                case "-docID":
                    docId = Integer.parseInt(args[++i]);
                    break;
                case "-field":
                    fieldName = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-order":
                    order = args[++i];
                    break;
                case "-outputfile":
                    outputFilePath = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (docId == -1 || indexPath == null || fieldName == null || top == -1 || (!order.equalsIgnoreCase("TF") && !order.equalsIgnoreCase("DF") && !order.equalsIgnoreCase("IDF") && !order.equalsIgnoreCase("TFXIDF"))) {
            System.err.println("Usage" + usage);
            System.exit(1);
        }

        if (!Files.isReadable(indexPath)){
            System.out.println("Documents does not exist or is not readable, please check the path");
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        read_args(args);

        IndexReader reader;
        Directory dir;
        try {
            dir = FSDirectory.open(indexPath);
            reader = DirectoryReader.open(dir);

            Terms terms = reader.getTermVector(docId, fieldName);
            TermsEnum termsEnum = terms.iterator();
            BytesRef text;


            while ((text=termsEnum.next()) != null) { // itera por todos los terminos del documento
                String term = text.utf8ToString();
                    termFrequencies.put(term, (int) termsEnum.totalTermFreq());
                    docFrequencies.put(term, reader.docFreq(new Term(fieldName, term)));
                    idFrequencies.put(term, getIDF(reader, text));
                    tf_idfFrequencies.put(term, termsEnum.totalTermFreq() * getIDF(reader, text));
                }


            reader.close();
            dir.close();

            FileWriter writer = null;

            if(outputFilePath != null) {
                writer = new FileWriter(outputFilePath);
            }

            String show = "Showing the top " + top + " terms for the field " + fieldName + " in the index " + indexPath + " ordered by " + order;

            switch (order) {
                case "TF":
                    Object[] tf = getIntegerSorted(termFrequencies);

                    if (outputFilePath != null && writer != null) {
                        System.out.println("Writing in " + outputFilePath);
                        printFrequencies(tf, true, writer);
                        writer.write("\n\n" + show + "\n");
                        writer.write(toMapString(getIntegerSorted(termFrequencies), top));
                    }else {
                        printFrequencies(tf, false, null);
                        System.out.println("\n\n" + show + "\n");
                        System.out.println(toMapString(getIntegerSorted(termFrequencies), top));
                    }
                    break;

                case "DF":
                    Object[] df = getIntegerSorted(docFrequencies);

                    if (outputFilePath != null && writer != null) {
                        System.out.println("Writing in " + outputFilePath);
                        printFrequencies(df, true, writer);
                        writer.write(show);
                        writer.write(toMapString(getIntegerSorted(docFrequencies), top));
                    }
                    else {
                        printFrequencies(df, false, null);
                        System.out.println("\n\n" + show + "\n");
                        System.out.println(toMapString(getIntegerSorted(docFrequencies), top));
                    }
                    break;

                case "IDF":
                    Object[] idf = getDoubleSorted(idFrequencies);

                    if (outputFilePath != null && writer != null) {
                        System.out.println("Writing in " + outputFilePath);
                        printFrequencies(idf, true, writer);
                        writer.write(show);
                        writer.write(toMapString(getDoubleSorted(idFrequencies), top));
                    }
                    else {
                        printFrequencies(idf, false, null);
                        System.out.println("\n\n" + show + "\n");
                        System.out.println(toMapString(getDoubleSorted(idFrequencies), top));
                    }
                    break;

                case "TFXIDF":
                    Object[] tf_idf = getDoubleSorted(idFrequencies);

                    if (outputFilePath != null && writer != null) {
                        System.out.println("Writing in " + outputFilePath);
                        printFrequencies(tf_idf, true, writer);
                        writer.write(show);
                        writer.write(toMapString(getDoubleSorted(tf_idfFrequencies), top));
                    }
                    else {
                        printFrequencies(tf_idf, false, null);
                        System.out.println("\n\n" + show + "\n");
                        System.out.println(toMapString(getDoubleSorted(tf_idfFrequencies), top));
                    }
                    break;
            }

            if (writer != null)
                writer.close();

        } catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }

    }

    private static void printFrequencies (Object[] array, boolean write, FileWriter writer) throws IOException {

        for (Object o : array){
            String term = o.toString().split("=")[0];
            String output = term.toUpperCase() +
                    "\t TF = " + termFrequencies.get(term) +
                    "\t DF = " + docFrequencies.get(term) +
                    "\t IDFLOG10 = " + idFrequencies.get(term) +
                    "\t\t TF X IDFLOG10 = " + tf_idfFrequencies.get(term);

            if (write){
                writer.write(output + "\n");
            } else {
                System.out.println(output);
            }
        }
    }

    public static String toMapString(Object[] array, int top) {

        if (top > array.length){
            System.out.println("This array has " + array.length + " elements and you asked for " + top + ", so it can not be fully displayed");
            return Arrays.toString(Arrays.copyOfRange(array, 0, array.length));
        } else
            return Arrays.toString(Arrays.copyOfRange(array, 0, top));
    }

    public static Object[] getIntegerSorted(Map<String, Integer> frequencies){
        Stream<Map.Entry<String, Integer>> ordenados = frequencies.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed());
        return ordenados.toArray();
    }

    public static Object[] getDoubleSorted(Map<String, Double> frequencies){
        Stream<Map.Entry<String, Double>> ordenados = frequencies.entrySet().stream()
                .sorted(Map.Entry.comparingByValue());
        return ordenados.toArray();
    }


    private static double getIDF(IndexReader reader, BytesRef bytes){
        int docCount = reader.numDocs();
        int docFreq;
        double score = 0;
        try {
            docFreq = reader.docFreq(new Term(fieldName, bytes));
            score = Math.log10((double) docCount / (double) docFreq);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new BigDecimal(score).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

}
