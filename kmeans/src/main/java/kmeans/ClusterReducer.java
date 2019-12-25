package kmeans;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This class produces clusters and writes the cluster to output file.
 */

public  class ClusterReducer
        extends Reducer<Text, ObjectWritable,Text,Text> {

    public void reduce(Text key, Iterable<ObjectWritable> values,
                       Context context
    ) throws IOException, InterruptedException {

        System.out.println("Reducer key : "+key+ " value : "+values);

        String output = "";
        for (ObjectWritable val : values) {
            if(!val.get().toString().equalsIgnoreCase("CustomerID")) {
                if (output.equals("")) {
                    output += val.get().toString();
                } else {
                    output += "," + val.get().toString();
                }
            }
            //System.out.println("Reducer iter kay : "+key+ " value : "+val);
        }

        System.out.println("Reducer iter key : "+key+ " value : "+output);

        if(!output.equals(""))
            context.write(key, new Text(output));

    }
}
