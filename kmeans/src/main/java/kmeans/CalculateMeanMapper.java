package kmeans;

import common.Customer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * This class calculates mean for the given class points
 */

public  class CalculateMeanMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * The standard map method that performs mean calculation.
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {

        // System.out.println("CalculateMeanMapper kay : "+key+ " value : "+value);

        String record [] = value.toString().split("\t");
        if(record.length >= 2) {
            record = record[1].split(",");

            HashMap<String, Customer> customers = getCustemrs(context);

            int gender = 0;
            float age = 0;
            float annualIncome = 0;
            float spendingScore = 0;

            for (int i = 0; i < record.length; i++) {
                Customer tmp = customers.get(record[i]);
                if (tmp.getGender().equalsIgnoreCase("male")) {
                    gender += 1;

                }
                age += tmp.getAge();
                annualIncome += tmp.getAnnualIncome();
                spendingScore += tmp.getSpendingScore();
            }

            System.out.println("Total Values : " + " age = " + age + " gender = " + gender + " annualIncome = " + annualIncome + " spendingScore = " + spendingScore + " record.length = " + record.length);

            gender = (int) (gender / record.length);
            age = age / record.length;
            annualIncome = (annualIncome / record.length);
            spendingScore = (spendingScore / record.length);

            String genderText = "female";

            if (gender >= 1) {
                genderText = "male";
            }

            String output = key.toString() + "," + genderText + "," + age + "," + annualIncome + "," + spendingScore;
            System.out.println("Calculated Output : " + output);
            context.write(new Text(key.toString()) , new Text(output));

        }

    }

    public HashMap<String, Customer> getCustemrs(Context context) {
        HashMap<String, Customer> results = new  HashMap<String, Customer>();
        try {

            Path pt = new Path("hdfs:///user/Mall_Customers.csv");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            try {
                int i = 0;
                String line;
                line = br.readLine();
                while (line != null ) {
                    //System.out.println(line);

                    // be sure to read the next line otherwise you'll get an infinite loop
                    line = br.readLine();
                    i++;

                    String[] record = line.split(",");

                    Customer mean = new Customer();
                    try {
                        mean.setCustomerId(record[0]);
                        mean.setGender(record[1]);
                        mean.setAge(Double.parseDouble(record[2]));
                        mean.setAnnualIncome(Double.parseDouble(record[3]));
                        mean.setSpendingScore(Double.parseDouble(record[4]));
                        results.put(mean.getCustomerId(),mean);
                    }catch(Exception e) {
                        // ignore
                        System.out.println("ignoring exception "+e);
                    }

                }
            } finally {
                // you should close out the BufferedReader
                br.close();
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

        return results;
    }


}
