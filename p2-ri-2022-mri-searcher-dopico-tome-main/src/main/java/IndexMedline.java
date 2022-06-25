import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Objects;
import java.util.stream.Collectors;

public class IndexMedline {

    public static void main(String[] args) {
        String usage = "java org.apache.lucene.demo.IndexMedline"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-openmode MODE] [-indexingmodel MODEL]\n\n";
        String indexPath = null;
        String docsPath = null;
        String openmode = null;
        String indexingModel = null;
        float lambda = 0;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-openmode": {
                    openmode = args[++i];
                    if (!openmode.equals("create") && !openmode.equals("append")
                            && !openmode.equals("create_or_append"))
                        throw new IllegalArgumentException("openmode incorrecto, valores posibles: " +
                                "create, append, create_or_append");
                    break;
                }
                case "-indexingmodel": {
                    indexingModel = args[++i];
                    if (indexingModel.equals("jm"))
                        lambda = Float.parseFloat(args[++i]);
                    else if (!indexingModel.equals("tfidf"))
                        throw new IllegalArgumentException("indexingmodel incorrecto, valores posibles:" +
                                "tfidf, jm lambda");
                    break;
                }
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" + docDir.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            assert indexPath != null;
            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            switch (indexingModel) {
                case "jm": {
                    LMJelinekMercerSimilarity jm = new LMJelinekMercerSimilarity(lambda);
                    iwc.setSimilarity(jm);
                    break;
                }
                case "tfidf": {
                    ClassicSimilarity tfidf = new ClassicSimilarity();
                    iwc.setSimilarity(tfidf);
                    break;
                }
                default: {
                    System.out.println("OPENMODE INCORRECTO LINEA 92");
                    System.exit(-2);
                }
            }

            switch (openmode) {
                case "create":
                    iwc.setOpenMode(OpenMode.CREATE);
                    break;
                case "create_or_append":
                    iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
                    break;
                case "append":
                    iwc.setOpenMode(OpenMode.APPEND);
                    break;
                default: {
                    System.out.println("OPENMODE INCORRECTO LINEA 109");
                    System.exit(-1);
                }
            }
            //todos los archivos estan en el mismo documento, separados por: .I X\n.W
            BufferedReader bufferedReader = new BufferedReader(new FileReader(
                    docsPath + "/" + Objects.requireNonNull(new File(docsPath).list())[0]));

            String contents = bufferedReader.lines().collect(Collectors.joining("\n"));
            System.out.println(contents);
            String[] subContents= contents.split("\\.I (\\d)+\n\\.W");//todo como introducir .W en la expr regular
            IndexWriter iWriter = new IndexWriter(dir, iwc);

            for (int i = 1; i < subContents.length; ++i)
                indexDoc(iWriter, subContents[i], i);

            iWriter.close();

            Date end = new Date();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                System.out.println("Indexed " + reader.numDocs() + " documents in " + (end.getTime() - start.getTime())
                        + " milliseconds");
            }
        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }
    }

    static void indexDoc(IndexWriter writer, String subFileCont, int subFileDocID) throws IOException {
        Document doc = new Document();

        doc.add(new StringField("docIDMedline", String.valueOf(subFileDocID), Field.Store.YES));

        doc.add(new TextField("contents", subFileCont, Field.Store.YES));

        if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
            System.out.println("adding " + "doc" + subFileDocID);
            writer.addDocument(doc);
        } else {
            System.out.println("updating " + "doc" + subFileDocID);
            writer.updateDocument(new Term("path", "doc" + subFileDocID), doc);
        }
    }
}
