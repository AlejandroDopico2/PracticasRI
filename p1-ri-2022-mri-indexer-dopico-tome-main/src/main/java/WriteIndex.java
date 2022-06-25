import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class WriteIndex {

    public static Path indexPath = null;
    public static String outputFilePath = null;

    private static void read_args(String[] args) {
        String usage = "java WriteIndex [-index INDEX_PATH] [-outputfile OUTPUTFILE_PATH]";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = Paths.get(args[++i]);
                    break;
                case "-outputfile":
                    outputFilePath = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (outputFilePath == null || indexPath == null) {
            System.err.println("Usage" + usage);
            System.exit(1);
        }

        if (!Files.isReadable(indexPath) || !Files.isReadable(Path.of(outputFilePath))) {
            System.out.println("Documents does not exist or is not readable, please check the path");
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        read_args(args);

        IndexReader reader = null;
        FileWriter writer = null;
        try {
            Directory dir = FSDirectory.open(indexPath);
            reader = DirectoryReader.open(dir);

            System.out.println(outputFilePath);
            writer = new FileWriter(outputFilePath);


            for (int i = 0; i < reader.numDocs(); i++) {
                Document doc = reader.document(i);
                List<IndexableField> list = doc.getFields();
                writer.write("Documento número " + i + "\n");
                for (IndexableField e : list) {
                    writer.write(e.name() + " = " + doc.get(e.name()) + "\n");
                }
                writer.write("\n\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null && writer != null) {
                    reader.close();
                    writer.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
