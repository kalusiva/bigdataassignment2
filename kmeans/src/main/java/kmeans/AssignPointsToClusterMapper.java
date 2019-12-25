package kmeans;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import common.Customer;

/**
 * This class assigns each point to the nearest centroid.
 */

public  class AssignPointsToClusterMapper
        extends Mapper<Object, Text, Text, ObjectWritable> {

    /**
     * This method maps the given data points to appropriate mean based on euclidean distance.
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        //System.out.println("Mapper kay : "+key+ " value : "+value);

        String record [] = value.toString().split(",");

        Customer tmp = new Customer();
        int minIndex = 0;

        try{
            tmp.setCustomerId(record[0]);
            tmp.setGender(record[1]);
            tmp.setAge(Double.parseDouble(record[2]));
            tmp.setAnnualIncome(Double.parseDouble(record[3]));
            tmp.setSpendingScore(Double.parseDouble(record[4]));


            ArrayList<Customer> means  = getClusterMeans(3,context) ;

            double minDistance = 0;

            for(int i=0;i<means.size();i++) {
                double tmpDistance = tmp.distance(means.get(i));
                System.out.println("distance = "+tmpDistance+" index="+i);
                if(i == 0) {
                    minDistance = tmpDistance;
                    minIndex = 0;
                }else {
                    if(minDistance > tmpDistance) {
                        minDistance = tmpDistance;
                        minIndex = i;
                    }
                }

                System.out.println("minIndex = "+minIndex);
            }

        }catch(Exception e) {
            // ignore
            System.out.println("ignoring exception "+e);
        }

        System.out.println("Mapping key :"+record[0]+ " to clsuter "+minIndex);

        context.write(new Text(""+minIndex) , new ObjectWritable(record[0]));
    }

    public ArrayList<Customer>  getClusterMeans(int k, Context context) {
        ArrayList<Customer> results = new  ArrayList<Customer>(3);
        Path pt = null;
        try {

            String fileName = context.getConfiguration().get("meansFile");

            if( fileName == null) {

                pt = new Path("hdfs:///user/Mall_Customers.csv");
            }else {
                System.out.println("fileName :"+fileName);
                pt = new Path(fileName);
            }

            System.out.println(" Reading ... fileName :"+fileName);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            try {
                int i = 0;
                String line;



                while ((line = br.readLine()) != null && i <= k) {
                    System.out.println("line "+i+" " +line);

                    // be sure to read the next line otherwise you'll get an infinite loop
                    if(fileName == null)
                        line = br.readLine();
                    i++;

                    if (i > 1 || (fileName != null)) {
                        String[] record = null;
                        if(fileName == null)
                            record = line.split(",");
                        else {
                            record  = line.toString().split("\t");
                            record = record[1].split(",");
                        }
                        Customer mean = new Customer();
                        try {
                            mean.setCustomerId(record[0]);
                            mean.setGender(record[1]);
                            mean.setAge(Double.parseDouble(record[2]));
                            mean.setAnnualIncome(Double.parseDouble(record[3]));
                            mean.setSpendingScore(Double.parseDouble(record[4]));
                            results.add(mean);

                            System.out.println("Adding Mean : "+i+" " +mean );

                        } catch (Exception e) {
                            // ignore
                            System.out.println("ignoring exception " + e);
                        }
                    }


                }
            } finally {
                // you should close out the BufferedReader
                br.close();
            }

        }catch(Exception e) {
            System.out.println("ignoring exception " + e);
            e.printStackTrace();
        }

        return results;
    }
}
