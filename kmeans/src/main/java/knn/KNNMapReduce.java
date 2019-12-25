package knn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import common.Customer;

/**
 * This class provides KNN based classification information for Mall Customer Problem.
 */

public class KNNMapReduce {

    private static final Log LOG = LogFactory.getLog(KNNMapReduce.class);


    /**
     * This is the main method that performs KNN on for output KMeans of MallCustomer.csv.
     * Assumption : KMeans output is processed using SummaryData and uploaded to /user/output.csv (arg[0].
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {

        int conusionMatrix [][] = new int[3][3];

        int k = 0;
        String outputFilebase = args[1];
        String outputFile = outputFilebase;
        ArrayList<Customer> custList = Customer.getCustomers(args[0]);

        int truepred = 0;
        int falsepred = 0;

        for(int i=0;i<5;i++) {
            //do {
            Configuration conf = new Configuration();
            if (k > 0) {
                conf.set("meansFile", outputFile + "out" + (k - 1) + "/part-r-00000");
            }

            int custIndex = ((int)(Math.random()*1000))%200;

            System.out.println("Customer Selected .."+custIndex);

            conf.set("testCustomer", custList.get(custIndex).toString());
            conf.set("k", "5");

            Job job = Job.getInstance(conf, "knn map reduce");
            job.setJarByClass(KNNMapReduce.class);
            job.setMapperClass(DistanceCalculator.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(NeighbourReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path p = new Path(outputFile + k);
            FileSystem fs = FileSystem.get(conf);
            fs.delete(p, true);

            FileOutputFormat.setOutputPath(job, new Path(outputFile + k));
            boolean status = job.waitForCompletion(true);

            int predClass = getPredictedClass(outputFile + k+"/part-r-00000");

            System.out.println("Customer Id : "+custList.get(custIndex).getCustomerId()+ " Pred Class : "+predClass+ " Actual Class : "+custList.get(custIndex).getCategory());

            if(custList.get(custIndex).getCategory() == predClass) {
                truepred++;
            }else {
                falsepred++;
            }

            conusionMatrix[predClass][custList.get(custIndex).getCategory()]+=1;

        }

        System.out.println("Confusion Matrix");

        for(int i=0;i<conusionMatrix.length;i++) {
            for(int j=0;j<conusionMatrix[i].length;j++) {
                System.out.print(conusionMatrix[i][j]+"\t");
            }
            System.out.println();
        }

        System.out.println("Accuracy : "+(truepred/(truepred+falsepred)));

    }

    /**
     * This method connects to hdfs and gets the output file and reads the predicted class
     * @param filePath
     * @return
     */

    public static int getPredictedClass(String filePath) {
        System.out.println("getPredictedClass ...");
        int category = 0;
        BufferedReader br = null;
        try {
            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(new Configuration());
            br = new BufferedReader(new InputStreamReader(fs.open(pt)));

            String line;
            if ((line = br.readLine()) != null) {
                System.out.println("Line = "+line);
                String tok [] = line.split("\t");

                if(tok.length >= 2) {


                    category = Integer.parseInt(tok[1]);

                    System.out.println("category = "+category);

                }else {
                    System.out.println("Not matching = ");

                }

            }else {
                System.out.println("Not matching = ");

            }
        }catch(Exception e) {
            e.printStackTrace(System.out);
        }finally {
            try {
                br.close();
            }catch(Exception ee) {
                System.out.println("static finally ignoring exception "+ee);
            }
        }

        return category;
    }

}