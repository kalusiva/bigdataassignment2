package knn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import common.Customer;

/**
 * This innner class gets all the disantce matrix and reduces to target class using the k-distance
 */

public class NeighbourReducer
        extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

        ArrayList<Customer> custList = new ArrayList<Customer>();

        System.out.println("Reducer key : "+key+ " value : "+values);

        for (Text val : values) {
            if(!val.toString().equalsIgnoreCase("CustomerID")) {


                custList.add(Customer.parseCustomer(val.toString()));

            }
        }

        Collections.sort(custList);

        int k = Integer.parseInt(context.getConfiguration().get("k"));

        int maxVoteCount = 0;
        int maxVoteCategory = 0;

        HashMap<Integer,Integer> votes = new HashMap<Integer, Integer>();

        for(int i=0;i<k;i++) {

            System.out.println("Cust id : "+custList.get(i).getCustomerId()+ " distance  : "+custList.get(i).getDistance());

            Integer count = votes.get(custList.get(i).getCategory());
            int tmpVotes = 0;
            if(count == null ) {
                tmpVotes = 1;
            }else {
                tmpVotes=count.intValue()+1;
            }

            votes.put(custList.get(i).getCategory(),new Integer(tmpVotes));

            if(tmpVotes > maxVoteCount ) {
                maxVoteCategory = custList.get(i).getCategory();
                maxVoteCount = tmpVotes;

                System.out.println("maxVoteCount : "+maxVoteCount + " Category : "+custList.get(i).getCategory());
            }

            System.out.println("Similar Customer : "+custList.get(i).toString());

        }

        context.write(key, new Text(""+maxVoteCategory));

    }
}