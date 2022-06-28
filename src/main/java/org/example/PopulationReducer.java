package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PopulationReducer extends Reducer<Text, Text, Text, IntWritable> {
    private final IntWritable result = new IntWritable();
    private Text countryName = new Text( "Unknown");

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int n = 0;
        countryName = new Text("country" + key.toString());
        for (Text val : values) {
            String strVal = val.toString();
            if (strVal.length() <= 10) {
                sum += Integer.parseInt(strVal);
                n += 1;
            } else {
                countryName = new Text(strVal);
            }
        }
//        if (n == 0) n = 1;
//        result.set(sum / n);
//        context.write(cityName, result);

        if (n != 0 && countryName.toString().compareTo("Unknown") != 0) {
            result.set(sum / n);
            context.write(countryName, result);

            // if (n =0){
            //     if (n=0)n=1;
            //     result.set(sum/n);
            //     context.write(cityName,result);
            // }

            // if(cityName.toString().compareTo("Unknown" ) ≠ 0){
            //     if(n=0)n=1;
            //     result.set(sum/n);
            //     context.write(cityName, result);
            // }

            // if(n ≠ 0){
            //     result.set(sum/n);
            //     context.write(cityName, result);
            // }

            // if (n = 0) n=1;
            // result.set(sum/n);
            // context.write(cityName, result);

        }
    }
}

