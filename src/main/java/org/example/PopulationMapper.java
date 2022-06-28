package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PopulationMapper extends Mapper<Object, Text, Text, Text> {
    public void map (Object key, Text value, Context context)
            throws IOException, InterruptedException{
        String txt = value.toString();
        String[] tokens = txt.split(",");
        String id = tokens[0].trim();
        String population = tokens[1].trim();
        if (population.compareTo("Population") != 0 )
            context.write(new Text(id), new Text(population));
    }
}
