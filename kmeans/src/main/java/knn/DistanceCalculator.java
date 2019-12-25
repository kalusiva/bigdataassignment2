package knn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import common.Customer;

import java.io.IOException;

/**
 * This class process each line from given input file and finds the distance between given line and input customer.
 */

public  class DistanceCalculator
        extends Mapper<Object, Text, Text, Text> {

    /**
     * This method performs distance calculation for given customer and line from dataset
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        if(!value.toString().startsWith("Customer")) {
            System.out.println("line = "+value.toString());
            Customer tmp = Customer.parseCustomer(value.toString());
            Customer targetCustomer = Customer.parseCustomer(context.getConfiguration().get("testCustomer"));
            //tmp.setCategory(targetCustomer.getCategory());
            tmp.setDistance(tmp.distance(targetCustomer));
            System.out.println(" Customer Id : "+tmp.getCustomerId()+ " class="+tmp.getCategory()+ " distance = "+tmp.getDistance());
            context.write(new Text(""+targetCustomer.getCustomerId()), new Text(tmp.toString()));
        }
    }

}