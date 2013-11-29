package fr.gautier.pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MapOutput extends MapReduceBase implements
        Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value,
            OutputCollector<DoubleWritable, Text> collector, Reporter reporter)
            throws IOException {
        
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        double pagerank = 0.0;
        String sourceUrl = "undefined";
        
        if (tokenizer.hasMoreTokens()) {
            pagerank = Double.parseDouble(tokenizer.nextToken());
        }
        
        if (tokenizer.hasMoreTokens()) {
            sourceUrl = tokenizer.nextToken();
        }
        
        collector.collect(new DoubleWritable(pagerank), new Text(sourceUrl));
    }
}
