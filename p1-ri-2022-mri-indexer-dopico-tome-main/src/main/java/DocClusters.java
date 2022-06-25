//https://stackabuse.com/guide-to-k-means-clustering-with-java/
//Usadas las clases KMeans.java y DataSet.java de Darinka Zobenica (https://github.com/Mentathiel).

import org.apache.commons.math3.linear.RealVector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;

public class DocClusters {

    public static String index = null;
    public static String field = null;
    public static String doc = null;
    public static String rep = null;
    public static int top = -1;
    public static int k = -1;
    public static RealVector vectorDocObjetivo = null;
    public static ArrayList<RealVector> arrayVectors = new ArrayList<>();

    private static void read_args(String[] args) {
        String usage = "java org.apache.lucene.demo.DocClusters"
                + " [-index index_path] [-field selectet_field] [-doc docID] [-top n] [-rep representation] number_of_clusters\n\n";
        int i;
        for (i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    index = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-doc":
                    doc = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-rep": {
                    rep = args[++i];
                    if (!rep.equals("bin") && !rep.equals("tf") && !rep.equals("tfxidf")) {
                        System.err.println("Valid -rep values: bin, tf or tfxidf\nUsage" + usage);
                        System.exit(3);
                    }
                    break;
                }
                default: {
                    if (i == 10)
                        k = Integer.parseInt(args[i]);
                    else {
                        System.err.println("Too many arguments\nUsage: " + usage);
                        System.exit(4);
                    }

                }
            }
        }


        if (index == null || field == null || doc == null || top == -1 || rep == null || k == -1) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        read_args(args);

        String[] argsSimilarDocs = new String[args.length - 1];
        System.arraycopy(args, 0, argsSimilarDocs, 0, args.length - 1);
        SimilarDocs.main(argsSimilarDocs);
        arrayVectors = SimilarDocs.arrayVectors;
        vectorDocObjetivo = SimilarDocs.vectorTargetDoc;

        ArrayList<RealVector> terminos = new ArrayList<>(arrayVectors);
        Date start = new Date();
        System.out.println("\n-------------------DOCCLUSTERS-------------------");
        try {
            File csv = new File("a.csv");
            FileWriter escritor = new FileWriter(csv);
            BufferedWriter buf = new BufferedWriter(escritor);
            int i;

            for (i = 0; i < terminos.get(0).getDimension() - 1; ++i)
                buf.write("Termino" + i + ",\t");
            buf.write("Termino" + i + "\n");

            double[] listaVect;
            for (int j = 0; j < top; j++ ){
                RealVector termino = terminos.get(j);
                listaVect = termino.toArray();
                for (i = 0; i < listaVect.length - 1; ++i)
                    buf.write(BigDecimal.valueOf(listaVect[i])
                            .setScale(2, RoundingMode.HALF_UP).doubleValue() + ",\t\t");
                buf.write(BigDecimal.valueOf(listaVect[i])
                        .setScale(2, RoundingMode.HALF_UP).doubleValue() + "\n");
            }

            buf.close();
            DataSet dataSet = new DataSet("a.csv");
            KMeans.kmeans(dataSet, k);
            if (!csv.delete())
                System.out.println("Imposible borrar csv autogenerado");

            System.out.println(dataSet);
            Date end = new Date();
            System.out.println("DocClusters in " + (end.getTime() - start.getTime()) + " milliseconds");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
