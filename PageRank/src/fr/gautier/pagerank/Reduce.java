package fr.gautier.pagerank;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Reduce extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {
    
    private static final double DAMPING_FACTOR = 0.85;
    
    private Text outputKey = new Text();
    
    private Text outputValue = new Text();
    
    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        
        String url = key.toString();
        StringBuffer buf = new StringBuffer();
        buf.append(url);
        
        double newPR = 1 - DAMPING_FACTOR;
        
        while(values.hasNext()) {
            String value = values.next().toString();
            
            try {
                double d = Double.valueOf(value);
                newPR += DAMPING_FACTOR * d;
            }
            catch(NumberFormatException e) {
                buf.append(" " + value);
            }
        }
        
        outputKey.set(String.valueOf(newPR));
        outputValue.set(buf.toString());
        
        output.collect(outputKey, outputValue);
    }
}
