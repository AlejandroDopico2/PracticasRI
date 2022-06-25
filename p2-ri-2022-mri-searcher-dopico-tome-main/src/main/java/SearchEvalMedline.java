import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
//TODO METRICA MAP
public class SearchEvalMedline {

    public static int cut = 0;
    public static int top = 0;
    public static String indexing = null;
    public static float acumP = 0;
    public static float acumRec = 0;
    public static float acumMap = 0;

    public static float sumP = 0;
    public static float sumR = 0;
    public static float sumM = 0;

    public static void main(String[] args) {
        String usage = "java org.apache.lucene.demo.SearchEvalMedline"
                + " [-search jm lambda | jm lambda] [-indexing pathname] [-cut n] [-top m] "
                + "[-queries all | int1 | int1-int2]\n\n";
        String queries = null;
        String search = null;
        float lambda = 0;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-indexing":
                    indexing = args[++i];
                    break;
                case "-cut":
                    cut = Integer.parseInt(args[++i]);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-queries": {
                    queries = args[++i];//comprobar si es: all, x, x-y
                    break;
                }
                case "-search": {
                    search = args[++i];
                    if (search.equals("jm"))
                        lambda = Float.parseFloat(args[++i]);
                    else if (!search.equals("tfidf"))
                        throw new IllegalArgumentException("search incorrecto, valores posibles:" +
                                "tfidf, jm lambda");
                    break;
                }
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (indexing == null || queries == null || search == null) {
            System.err.println("Usage: " + usage);
            System.exit(-2);
        }

        try {
            Directory dir = FSDirectory.open(Paths.get(indexing));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            String sJm = null;
            LMJelinekMercerSimilarity jm = null;
            switch (search) {
                case "jm": {
                    jm = new LMJelinekMercerSimilarity(lambda);
                    iwc.setSimilarity(jm);
                    sJm = "lambda." + lambda;
                    break;
                }
                case "tfidf": {
                    ClassicSimilarity tfidf = new ClassicSimilarity();
                    iwc.setSimilarity(tfidf);
                    sJm = "";
                    break;
                }
                default: {
                    System.out.println("OPENMODE INCORRECTO LINEA 71");
                    System.exit(-2);
                }
            }
            IndexReader reader = DirectoryReader.open(dir);
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(jm);
            QueryParser parser = new QueryParser("contents", analyzer);

            BufferedReader bufferedReader = new BufferedReader(new FileReader("query/MED.QRY"));
            String contents = bufferedReader.lines().collect(Collectors.joining());
            String[] subQueries = contents.split("\\.I (\\d)+\\.W");

            //subQueries[0] es un espacio en blanco, se salta
            int ini;
            int fin;
            if (queries.equals("all")) {
                ini = 1;
                fin = subQueries.length;
            } else {
                if (queries.contains("-")) {
                    String[] limites = queries.split("-");
                    ini = Integer.parseInt(limites[0]);
                    fin = Integer.parseInt(limites[1]);
                } else {
                    ini = Integer.parseInt(queries);
                    fin = ini + 1;
                }
            }

            File salida = new File("medline." + search + "." + cut + ".hits." + sJm + ".q" + queries + ".txt");
            FileWriter escritorS = new FileWriter(salida);
            BufferedWriter bufS = new BufferedWriter(escritorS);

            File csv = new File("medline." + search + "." + cut + ".cut." + sJm + ".q" + queries + ".csv");
            FileWriter escritorCsv = new FileWriter(csv);
            BufferedWriter bufCsv = new BufferedWriter(escritorCsv);

            bufCsv.write("queryID, P@" + cut + ", RECALL@" + cut + ", APN@" + cut + "\n");

            float sumP = 0, sumR = 0, sumM = 0;
            for  (int i = ini; i < fin; i++) {
                getResults(subQueries[i], i, searcher, parser, bufCsv, bufS);
            }
            int nQueries = subQueries.length;
            System.out.println("Métricas promedio de las queries:");
            bufS.write("Métricas promedio de las queries");
            System.out.println("P@n promedio = " + (acumP/cut) +
                    "\nRecall@n promedio = " + (acumRec/nQueries) +
                    "\nMAP@n promedio = " + (acumMap/nQueries) + "\n");
            bufS.write("P@n promedio = " + (acumP/nQueries) +
                            "\nRecall@n promedio = " + (acumRec/nQueries) +
                            "\nMAP@n promedio = " + (acumMap/nQueries) + "\n");

            System.out.println("Writing the average in csv");
            bufCsv.write("Mean, " + acumP/(fin-ini) + ", " + acumRec/(fin-ini) + ", " + acumMap/nQueries);
            bufCsv.close();
            bufS.close();
            escritorCsv.close();

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<Integer> countLineBufferedReader(String fileName, int idQuery) {
        ArrayList<Integer> relevantDocsID = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null)
                if (line.matches(idQuery + " .*"))
                    relevantDocsID.add(Integer.valueOf(line.split(" ")[2]));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return relevantDocsID;
    }

    public static void getResults(String query, int queryID, IndexSearcher searcher, QueryParser parser,
                                  BufferedWriter buf, BufferedWriter bufS) throws ParseException, IOException {
        int relevantRetrieved = 0;
        float sum = 0;
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexing)));

        //  Pasamos a minusculas y entrecomillamos los parentesis
        query = query.toLowerCase();
        query = query.replace("(", "\"(\"");
        query = query.replace(")", "\")\"");
        query = query.substring(1);
        TopDocs topDocs = searcher.search(parser.parse(query), Math.max(top, cut));

        ArrayList<Integer> relevantDocsID = countLineBufferedReader("medline/MED.REL", queryID);

        for (int j = 0; j < topDocs.scoreDocs.length; ++j) {
            if (relevantDocsID.contains(topDocs.scoreDocs[j].doc)) {
                relevantRetrieved++;
                sum = (float) relevantRetrieved / j;
            }
        }

        float precision = (float) relevantRetrieved / topDocs.scoreDocs.length;
        float recall = (float) relevantRetrieved / relevantDocsID.size();
        float apn = sum / relevantDocsID.size();
        buf.write(queryID + ",");
        buf.write(precision + ",");
        buf.write(recall + ",");
        buf.write(String.valueOf(apn));

        System.out.println("Top docs para la query " + queryID);
        bufS.write("Top docs para la query " + queryID + "\n");
        for (int j = 0; j < topDocs.scoreDocs.length; ++j) {
            System.out.println("-> documento " + topDocs.scoreDocs[j].doc + ":");
            bufS.write("-> documento " + topDocs.scoreDocs[j].doc + ":" + "\n");
            System.out.println("\tcampos del indice:");
            bufS.write("\tcampos del indice:" + "\n");
            List<IndexableField> campos = reader.document(topDocs.scoreDocs[j].doc).getFields();
            for (IndexableField campo : campos) {
                System.out.println("\t\t" + campo.name() + " = " + campo.stringValue());
                bufS.write("\t\t" + campo.name() + " = " + campo.stringValue() + "\n");
            }
            System.out.println("\tscore: " + topDocs.scoreDocs[j].score);
            bufS.write("\tscore: " + topDocs.scoreDocs[j].score + "\n");
            if (relevantDocsID.contains(topDocs.scoreDocs[j].doc)) {
                System.out.println("\tRELEVANTE");
                bufS.write("\tRELEVANTE" + "\n");
            }
            else {
                System.out.println("\tNO RELEVANTE");
                bufS.write("\tNO RELEVANTE" + "\n");
            }
        }
        sumP += (float) relevantRetrieved / topDocs.scoreDocs.length;
        System.out.println("\nMétricas para la query:");
        bufS.write("\nMétricas para la query:" + "\n");
        System.out.println("P@n para " + cut + " docs es "
                + (float) relevantRetrieved / topDocs.scoreDocs.length);
        bufS.write("P@n para " + cut + " docs es "
                + (float) relevantRetrieved / topDocs.scoreDocs.length + "\n");
        acumP += (float) relevantRetrieved / topDocs.scoreDocs.length;
        System.out.println("Recall@n para " + cut + " docs es "
                + (float) relevantRetrieved / relevantDocsID.size());
        bufS.write("Recall@n para " + cut + " docs es "
                + (float) relevantRetrieved / relevantDocsID.size() + "\n");
        acumRec += (float) relevantRetrieved / relevantDocsID.size();
        System.out.println("MAP@n para " + cut + " docs es "
                +  sum / relevantDocsID.size());
        bufS.write("MAP@n para " + cut + " docs es "
                +  sum / relevantDocsID.size() + "\n");
        if (!Float.isInfinite(sum))
            acumMap += sum / relevantDocsID.size();
        System.out.println("--------------------------------------------------");
        bufS.write("--------------------------------------------------" + "\n");

        buf.write("\n");
    }
}
