package fr.gautier.pagerank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class Map extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    @Override
    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        double pageRank = 0;
        String sourceUrl = "";
        
        if(tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            
            try {
                pageRank = Double.parseDouble(token);
            }
            catch(NumberFormatException e) {
                pageRank = 1.0;
                sourceUrl = token;
            }
        }
        else {
            throw new RuntimeException("Malformed input file");
        }
        
        if(sourceUrl.isEmpty()) {
            if(tokenizer.hasMoreTokens()) {
                sourceUrl = tokenizer.nextToken();
            }
            else {
                throw new RuntimeException("Map1 : malformed input file "
                        + "(missing sourceUrl)");
            }
        }
        
        Set<String> outlinks = new HashSet<String>();
        
        while(tokenizer.hasMoreTokens()) {
            outlinks.add(tokenizer.nextToken());
        }
        
        for(String link : outlinks) {
            outputKey.set(sourceUrl);
            outputValue.set(link);
            output.collect(outputKey, outputValue);
            
            outputKey.set(link);
            outputValue.set(String.valueOf(pageRank / outlinks.size()));
            output.collect(outputKey, outputValue);
        }
    }
}
