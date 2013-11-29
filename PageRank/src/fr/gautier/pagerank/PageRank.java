package fr.gautier.pagerank;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class PageRank {
    
    private static final int DEFAULT_NB_RUNS = 5;

    public static void main(String[] args) {
        String dir = null;
        int nbRuns = DEFAULT_NB_RUNS;
        
        if(args.length < 1) {
            System.err.println("Erreur : paramètre(s) manquant(s).");
            System.err.println("Paramètres requis : ");
            System.err.println("\t - dir");
        }
        else {
            dir = args[0];
            
            if(dir.endsWith("/") == false) {
                dir += "/";
            }
            
            try {
                if(args.length > 1) {
                    nbRuns = Integer.valueOf(args[2]);
                }
                
                int iter = 0;
                
                while(iter < nbRuns) {
                    computePageRank(dir + "iter" + iter,
                            dir + "iter" + (iter + 1));
                    
                    ++iter;
                }
                
                output(dir + "iter" + iter, dir + "output");
            }
            catch(NumberFormatException e) {
                System.err.println("Erreur : paramètre non valide (nbRuns).");
            }
            catch (IOException e) {
                System.err.println("Erreur lors de l'execution du job :\n");
                e.printStackTrace(System.err);
            }
        }
    }
    
    private static void computePageRank(final String inputPath,
            final String outputPath) throws IOException {
        
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("computePageRank");
        conf.setJarByClass(PageRank.class);
        
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Combine.class);
        conf.setReducerClass(Reduce.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        JobClient.runJob(conf);
    }
    
    private static void output(final String inputPath,
            final String outputPath) throws IOException {
        
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("Output");
        conf.setJarByClass(PageRank.class);
        
        conf.setMapperClass(MapOutput.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.setOutputKeyClass(DoubleWritable.class);
        conf.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        JobClient.runJob(conf);
    }
}
