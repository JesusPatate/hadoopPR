package fr.gautier.pagerank;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Reduce1 extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {
    
    private static final double DAMPING_FACTOR = 0.85;
    
    private Text outputKey = new Text();
    
    private Text outputValue = new Text();
    
    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        
        String url = key.toString();
        StringBuffer inlinks = new StringBuffer();
        
        double newPR = 1 - DAMPING_FACTOR;
        
        while(values.hasNext()) {
            String value = values.next().toString();
            StringTokenizer tokenizer = new StringTokenizer(value);
            String inlink = "";
            double pageRank = 0;
            int nbOutlinks = 0;
            
            if(tokenizer.hasMoreTokens()) {
                inlink = tokenizer.nextToken();
            }
            else {
                throw new RuntimeException("Reduce1 : malformed input file "
                        + "(missing inlink)");
            }
            
            if(tokenizer.hasMoreTokens()) {
                pageRank = Double.parseDouble(tokenizer.nextToken());
            }
            else {
                throw new RuntimeException("Reduce1 : malformed input file "
                        + "(missing pageRank)");
            }
            
            if(tokenizer.hasMoreTokens()) {
                nbOutlinks = Integer.parseInt(tokenizer.nextToken());
            }
            else {
                throw new RuntimeException("Reduce1 : malformed input file "
                        + "(missing nbOutlinks)");
            }
            
            newPR += DAMPING_FACTOR * (pageRank / nbOutlinks);
            inlinks.append(" " + inlink);
        }
        
        outputKey.set(url);
        outputValue.set(newPR + " " + inlinks.toString());
        
        output.collect(outputKey, outputValue);
    }
}
