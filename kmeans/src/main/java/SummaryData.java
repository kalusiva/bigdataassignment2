import java.io.*;
import java.util.HashMap;

public class SummaryData {
    private static boolean normalize = false;
    public static void main(String s[]) throws Exception {

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/kt/Documents/Kalu/Mall_Customers.csv")));
        String line = null;

        HashMap<String, String> data = new HashMap<String,String>();

        while(( line=br.readLine()) != null) {
            System.out.println("line = "+line);
            String tok [] = line.split(",");

            if(!normalize)
                data.put(tok[0],line);
            else {
                String targetLine = "";
                String gender = "";
                for(int i=0;i<tok.length;i++) {
                    if(tok[i].equalsIgnoreCase("Male")) {
                        gender="1";
                    }else if(tok[i].equalsIgnoreCase("FeMale") ) {
                        gender="0";
                    }
                    else {
                        if(i == 0) {
                            targetLine+=tok[i];
                        }else {
                            targetLine += "," + tok[i];
                        }
                    }
                }
                targetLine+=","+gender;
                gender="";
                data.put(tok[0], targetLine);
            }

        }
        br.close();

        br = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/kt/Documents/Kalu/bigdata/hadoop-2.9.2/output/output89/part-r-00000")));


        PrintWriter bw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("/Users/kt/Documents/Kalu/output.csv")));
        if(!normalize)
            bw.println("CustomerID,Gender,Age,Annual Income (k$),Spending Score (1-100),Category");
        else
            bw.println("CustomerID,Age,Annual Income (k$),Spending Score (1-100),Gender,Category");

        while(( line=br.readLine()) != null) {
            System.out.println("line = "+line);
            String record [] = line.toString().split("\t");
            String type=record[0];
            String tok [] = record[1].split(",");
            for(int i=0;i<tok.length;i++) {
                String lineRes = data.get(tok[i]);
                bw.println(lineRes+","+type);

            }

        }
        bw.close();
        br.close();

    }

}
