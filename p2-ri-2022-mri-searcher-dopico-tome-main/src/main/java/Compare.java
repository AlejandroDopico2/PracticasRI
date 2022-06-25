import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Compare {
    public static void main(String[] args) throws IOException {
        String usage = "java org.apache.lucene.demo.Compare"
                + " -result [primer_csv segundo_csv] -test [t|wilcoxon alfa]" + "\n\n";
        String primer_csv = null;
        String segundo_csv = null;
        String test = null;
        double  alfa = -1;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-results": {
                    primer_csv = args[++i];
                    segundo_csv = args[++i];
                    break;
                }
                case "-test": case "wilcoxon": {
                    test = args[++i];
                    alfa = Double.parseDouble(args[++i]);
                    break;
                }
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (primer_csv == null || segundo_csv == null || test == null) {
            throw new IllegalArgumentException("Usage: " + usage);
        }


        File primerCsv = new File(primer_csv);
        FileReader lectorP = new FileReader(primerCsv);
        BufferedReader bufP = new BufferedReader(lectorP);
        FileReader lectorP2 = new FileReader(primerCsv);
        BufferedReader bufP2 = new BufferedReader(lectorP2);
        long lineasPrimero = bufP2.lines().count();
        double[] atribPrimero = new double[5000];//1000 temporal
        ArrayList<String> elem = new ArrayList<>();
        bufP.readLine();//para no añadir la primera linea
        for (int i = 1; i < lineasPrimero - 1; ++i) {
            elem.addAll(List.of(bufP.readLine().split(",")));
            for (int j = 0; j < elem.size(); ++j)
                atribPrimero[j] = (Double.parseDouble(elem.get(j)));
        }


        File segundoCsv = new File(segundo_csv);
        FileReader lectorS = new FileReader(segundoCsv);
        BufferedReader bufS = new BufferedReader(lectorS);
        FileReader lectorS2 = new FileReader(segundoCsv);
        BufferedReader bufS2 = new BufferedReader(lectorS2);
        long lineasSegundo =  bufS2.lines().count();
        double[] atribSegundo = new double[5000];
        bufS.readLine();
        for (int i = 1; i < lineasSegundo - 1; ++i) {
            elem.addAll(List.of(bufS.readLine().split(",")));
            for (int j = 0; j < elem.size(); ++j)
                atribSegundo[j] = Double.parseDouble(elem.get(j));
        }

        if (test.equals("t")) {
            TTest ttest = new TTest();
            double res = ttest.pairedTTest(atribPrimero, atribSegundo);
            if (alfa < res)
                System.out.println("se rechaza la hipotesis nula, no es satisfactorio");
            else
                System.out.println("satisfactorio, comparacion t: " + ttest.pairedTTest(atribPrimero, atribSegundo));
        } else {
            WilcoxonSignedRankTest wilc = new WilcoxonSignedRankTest();
            double res = wilc.wilcoxonSignedRank(atribPrimero, atribSegundo);
            if (alfa < res)
                System.out.println("se rechaza la hipotesis nula, no es satisfactorio");
            else
                System.out.println("satisfactorio, comparacion wilcoxon: " + wilc.wilcoxonSignedRank(atribPrimero, atribSegundo));
        }
    }
}
