package fr.gautier.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Combine extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> collector, Reporter reporter)
            throws IOException {
        
        double intermediateValue = 0.0;
        
        while(values.hasNext()) {
            String valueStr = values.next().toString();
            
            try {
                double value = Double.valueOf(valueStr);
                intermediateValue += value;
            }
            catch(NumberFormatException e) {
                collector.collect(key, new Text(valueStr));
            }
        }
        
        collector.collect(key, new Text(String.valueOf(intermediateValue)));
    }
    
}
