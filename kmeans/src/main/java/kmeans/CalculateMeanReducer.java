package kmeans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This class writes the calculated mean to the output.
 */

public class CalculateMeanReducer
        extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

        //System.out.println("CalculateMeanReducer kay : "+key+ " value : "+values);

        String output = "";
        for (Text val : values) {

            //System.out.println("CalculateMeanReducer iter kay : "+key+ " value : "+val.toString());
            output=val.toString();
        }

        //System.out.println("CalculateMeanReducer iter kay : "+key+ " value : "+output);

        context.write(key, new Text(output));
    }
}
