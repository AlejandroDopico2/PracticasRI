import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

public class IndexFilesMultiThread {

    public static final FieldType TYPE_STORED_TOKENIZED = new FieldType();
    public static final FieldType TYPE_STORED = new FieldType();
    public static final FieldType TYPE_NON_STORED = new FieldType();
    public static boolean create;
    public static String indexPath = null;
    public static Directory directory = null;
    public static IndexWriter iWriter = null;
    public static int deep = -1;
    public static boolean partialIndexes = false;
    public static String[] onlyFilesToken = null;
    public static String onlyTopLines = null;
    public static String onlyBottomLines = null;
    static Path docDir = null;
    static int numThreads = 0;
    static ArrayList<Path> partialIndexPaths = new ArrayList<>();

    static {
        TYPE_STORED_TOKENIZED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        TYPE_STORED_TOKENIZED.setTokenized(true);
        TYPE_STORED_TOKENIZED.setStored(true);
        TYPE_STORED_TOKENIZED.setStoreTermVectors(true);
        TYPE_STORED_TOKENIZED.setStoreTermVectorPositions(true);
        TYPE_STORED_TOKENIZED.freeze();

        TYPE_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        TYPE_STORED.setTokenized(false);
        TYPE_STORED.setStored(true);
        TYPE_STORED.setStoreTermVectors(true);
        TYPE_STORED.setStoreTermVectorPositions(true);
        TYPE_STORED.freeze();

        TYPE_NON_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        TYPE_NON_STORED.setTokenized(true);
        TYPE_NON_STORED.setStored(false);
        TYPE_NON_STORED.setStoreTermVectors(true);
    }

    static void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            if (deep == 0) {
                System.out.println("Profundidad 0: no se indexa ningún documento.");
            }
            else if (deep > 0) {
                EnumSet<FileVisitOption> opciones = EnumSet.of(FOLLOW_LINKS);
                Files.walkFileTree(path, opciones, deep, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        try {
                            indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                        } catch (@SuppressWarnings("unused") IOException e) {
                            e.printStackTrace(System.err);
                            // don't index files that can't be read.
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } else {
                Files.walkFileTree(path, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        try {
                            indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                        } catch (@SuppressWarnings("unused") IOException e) {
                            e.printStackTrace(System.err);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } else {
            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
        }
    }

    static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
        if (new File(file.toString()).isDirectory())
            return;

        if (!validExt(file))
            return;

        try (InputStream stream = Files.newInputStream(file)) {
            Document doc = new Document();

            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);

            doc.add(new LongPoint("modified", lastModified));

            String contents = obtainContents(stream);

            doc.add(new Field("contents", contents, TYPE_NON_STORED));

            doc.add(new Field("contentsStored", contents, TYPE_STORED_TOKENIZED));

            doc.add(new Field("hostname", InetAddress.getLocalHost().getHostName(), TYPE_STORED));

            doc.add(new Field("thread", Thread.currentThread().getName(), TYPE_STORED));

            BasicFileAttributes at = Files.readAttributes(file, BasicFileAttributes.class);
            String type;
            if (at.isDirectory()) type = "isDirectory";
            else if (at.isRegularFile()) type = "isRegularFile";
            else if (at.isSymbolicLink()) type = "iSSymbolicLink";
            else if (at.isOther()) type = "isOther";
            else type = "error";

            doc.add(new Field("type", type, TYPE_STORED));

            doc.add(new LongPoint("sizeKB", at.size()));

            String patron = "yyyy-MM-dd HH:mm:ss";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(patron);

            FileTime creationTime = at.creationTime();
            String creationTimeFormateado = simpleDateFormat.format(new Date(creationTime.toMillis()));
            doc.add(new Field("creationTime", creationTimeFormateado, TYPE_STORED));

            FileTime lastAccessTime = at.lastAccessTime();
            String lastAccessTimeFormateado = simpleDateFormat.format(new Date(lastAccessTime.toMillis()));
            doc.add(new Field("lastAccessTime", lastAccessTimeFormateado, TYPE_STORED));

            FileTime lastModifiedTime = at.lastModifiedTime();
            String lastTimeModifiedTimeFormateado = simpleDateFormat.format(new Date(lastModifiedTime.toMillis()));
            doc.add(new Field("lastModifiedTime", lastTimeModifiedTimeFormateado, TYPE_STORED));

            Date creationTimelucene = new Date(creationTime.toMillis());
            String s1 = DateTools.dateToString(creationTimelucene, DateTools.Resolution.MILLISECOND);
            doc.add(new Field("creationTimeLucene", s1, TYPE_STORED));

            Date lastAccessTimelucene = new Date(lastAccessTime.toMillis());
            String s2 = DateTools.dateToString(lastAccessTimelucene, DateTools.Resolution.MILLISECOND);
            doc.add(new Field("lastAccessTimeLucene", s2, TYPE_STORED));

            Date lastModifiedTimelucene = new Date(lastModifiedTime.toMillis());
            String s3 = DateTools.dateToString(lastModifiedTimelucene, DateTools.Resolution.MILLISECOND);
            doc.add(new Field("LastModifiedTimeLucene", s3, TYPE_STORED));

            if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                System.out.println("adding " + file);
                writer.addDocument(doc);
            } else {
                System.out.println("updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }

    public static boolean validExt(Path file) {
        boolean correctExt = false;
        String fileExt = file.toString().substring(file.toString().lastIndexOf("."));
        Iterator<String> onlyFilesIterator = Arrays.stream(onlyFilesToken).iterator();
        while (onlyFilesIterator.hasNext()) {
            if (onlyFilesIterator.next().equals(fileExt)) {
                correctExt = true;
                break;
            }
        }

        return correctExt;
    }

    public static String obtainContents(InputStream stream) {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        String contents = "";
        List<String> linesList = null;

        int topLines = -1;
        int bottomLines = -1;
        int sizeList = -1;

        if (onlyTopLines != null)
            topLines = Integer.parseInt(onlyTopLines);
        if (onlyBottomLines != null)
            bottomLines = Integer.parseInt(onlyBottomLines);

        if (topLines != -1 || bottomLines != -1) {
            linesList = bufferedReader.lines().collect(Collectors.toList());
            sizeList = linesList.size();
            topLines = Integer.parseInt(onlyTopLines);
            bottomLines = Integer.parseInt(onlyTopLines);

            if (topLines + bottomLines > sizeList)
                bottomLines = sizeList - topLines;
        }
        else
            contents = bufferedReader.lines().collect(Collectors.joining("\n"));

        if (onlyTopLines != null) {
            for (int i = 0; i < topLines && i < sizeList; i++)
                contents = contents.concat(linesList.get(i) + "\n");
        }
        if (onlyBottomLines != null) {
            for (int i = (sizeList - bottomLines + 1); i < sizeList; i++)
                contents = contents.concat(linesList.get(i) + "\n");
        }
        return contents;
    }

    private static void read_args(String[] args) {
        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-numThreads nThread]\n\n";
        String docsPath = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-update":
                    create = false;
                    break;
                case "-create":
                    create = true;
                    break;
                case "-numThreads":
                    numThreads = Integer.parseInt(args[++i]);
                    break;
                case "-deep":
                    deep = Integer.parseInt(args[++i]);
                    break;
                case "-partialIndexes":
                    partialIndexes = true;
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        if (docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" + docDir.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

    }

    public static void deleteDirectories(String filePath) throws IOException {
        Path path = Paths.get(filePath);

        Files.walkFileTree(path,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    public static void main(String[] args) {

        read_args(args);

        if (numThreads == 0)
            numThreads = Runtime.getRuntime().availableProcessors();

        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        if (create) {
            try {
                deleteDirectories(indexPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        Date start = new Date();

        try {
            Directory dir;
            if (!partialIndexes) {
                dir = FSDirectory.open(Paths.get(indexPath));
                directory = dir;
                Analyzer analyzer = new StandardAnalyzer();
                IndexWriterConfig config = new IndexWriterConfig(analyzer);
                if (create)
                    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                else
                    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
                iWriter = new IndexWriter(directory, config);
            }

            Properties properties = new Properties();
            properties.load(new FileReader("src/main/resources/config.properties"));
            onlyFilesToken = properties.getProperty("onlyFiles").split(" ");
            onlyTopLines = properties.getProperty("onlyTopLines");
            onlyBottomLines = properties.getProperty("onlyBottomLines");


            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir)) {

                for (final Path path : directoryStream) {
                    if (Files.isDirectory(path)) {
                        Path partialIndexPath = null;
                        if (partialIndexes) {
                            partialIndexPath = Paths.get(indexPath + "/" + path.getFileName());
                            System.out.println("Partial index to be created in " + partialIndexPath);
                            dir = FSDirectory.open(partialIndexPath);
                            Analyzer analyzer = new StandardAnalyzer();
                            IndexWriterConfig config = new IndexWriterConfig(analyzer);
                            if (create)
                                config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                            else
                                config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
                            iWriter = new IndexWriter(dir, config);
                        }
                        final Runnable worker = new WorkerThread(null, path, iWriter, partialIndexPath);
                        executor.execute(worker);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        executor.shutdown();

        try {
            boolean awaited = executor.awaitTermination(1, TimeUnit.HOURS);
            if (!awaited)
                System.out.println("awaited failed");
            iWriter.close();

            if (partialIndexes) {
                System.out.println("All partial index has been created, now they are going to be merged in one at " + indexPath);
                directory = FSDirectory.open(Paths.get(indexPath));
                IndexWriterConfig iConfig = new IndexWriterConfig(new StandardAnalyzer());
                IndexWriter iFusedWriter = new IndexWriter(directory, iConfig);
                for (Path path : partialIndexPaths) {
                    iFusedWriter.addIndexes(FSDirectory.open(path));
//                    deleteDirectories(path.toString());
                }
                iFusedWriter.commit();
                iFusedWriter.close();
            }

            System.out.println("Finished all threads");
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Date end = new Date();
        try (IndexReader reader = DirectoryReader.open(directory)) {
            System.out.println("Indexed " + reader.numDocs() + " documents in " + (end.getTime() - start.getTime())
                    + " milliseconds");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class WorkerThread implements Runnable {
        Path path;
        IndexWriter iWriter;
        String thread;
        Path partialIndexPath;

        public WorkerThread(String thread, Path path, IndexWriter iWriter, Path partialIndexPath) {
            this.thread = thread;
            this.path = path;
            this.iWriter = iWriter;
            this.partialIndexPath = partialIndexPath;
        }

        public void setThread(String thread) {
            this.thread = thread;
        }

        public Path getPath() {
            return this.partialIndexPath;
        }

        @Override
        public void run() {

            try {
                setThread(Thread.currentThread().getName());

                if (partialIndexes) {
                    partialIndexPaths.add(getPath());

                    System.out.println("Thread " + thread + " indexing partially to directory " + partialIndexPath + "...");
                } else
                    System.out.println("Thread " + thread + " indexing to directory " + indexPath + "...");

                indexDocs(iWriter, path);

                if (partialIndexes)
                    iWriter.close();

            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
