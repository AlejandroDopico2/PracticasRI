import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class StatsField {
    public static String index = null;
    public static String field = null;
    public static Directory directory = null;


    private static void read_args(String[] args) {
        String usage = "java org.apache.lucene.demo.StatsField"
                + " [-index INDEX_PATH] [-field SELECTED_FIELD]\n\n";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    index = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        if (index == null) {
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
            IndexSearcher searcher = new IndexSearcher(reader);
            CollectionStatistics collectionStatistics;
            if (field != null) {
                collectionStatistics = searcher.collectionStatistics(field);
                System.out.println("Número total de documentos que tienen al menos un término para este campo: "
                        + collectionStatistics.docCount()
                        + "\nNombre del campo: " + collectionStatistics.field()
                        + "\nNúmero total de documentos: " + collectionStatistics.maxDoc()
                        + "\nNúmero total de postings para el campo: " + collectionStatistics.sumDocFreq()
                        + "\nNúmero total de tokens para el campo: " + collectionStatistics.sumTotalTermFreq());
            } else {
                for (final FieldInfo fieldInfo : FieldInfos.getMergedFieldInfos(reader)) {
                    String campo = fieldInfo.name;
                    System.out.println(campo);//TODO nulo en collectionStatistics
                    if ((collectionStatistics = searcher.collectionStatistics(campo)) != null) {
                        System.out.println("Número total de documentos que tienen al menos un término para este campo: "
                                + collectionStatistics.docCount()
                                + "\nNombre del campo: " + collectionStatistics.field()
                                + "\nNúmero total de documentos: " + collectionStatistics.maxDoc()
                                + "\nNúmero total de postings para el campo: " + collectionStatistics.sumDocFreq()
                                + "\nNúmero total de tokens para el campo: " + collectionStatistics.sumTotalTermFreq()
                                + "\n--------------------------------------------------");
                    }
                }
            }
            dir.close();
            directory.close();
            reader.close();

        } catch (IOException e) {
            System.out.println("Error: no existe el directorio" + index);
            e.printStackTrace();
        }

    }
}


























