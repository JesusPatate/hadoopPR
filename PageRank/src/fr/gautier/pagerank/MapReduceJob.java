package fr.gautier.pagerank;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.bloom.RemoveScheme;


public class MapReduceJob {
    
    private static final int DEFAULT_NB_RUNS = 5;

    public static void main(String[] args) {
        String inputPath = null;
        String outputPath = null;
        int nbRuns = DEFAULT_NB_RUNS;
        
        if(args.length < 2) {
            System.err.println("Erreur : paramètre(s) manquant(s).");
            System.err.println("Paramètres requis : ");
            System.err.println("\t - inputPath");
            System.err.println("\t - outputPath");
        }
        else {
            inputPath = args[0];
            outputPath = args[1];
            
            if(inputPath.endsWith("/") == false) {
                inputPath += "/";
            }
            
            if(outputPath.endsWith("/") == false) {
                outputPath += "/";
            }
            
            try {
                if(args.length > 2) {
                    nbRuns = Integer.valueOf(args[2]);
                }
                
                for(int i = 0 ; i < nbRuns ; ++i) {
                    runMapReduceJob(inputPath + "iter" + i,
                            outputPath + "iter" + (i + 1));
                }
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
    
    private static void runMapReduceJob(final String inputPath,
            final String outputPath) throws IOException {
        
        JobConf conf = new JobConf(MapReduceJob.class);
        conf.setJobName("PgeRank");
        
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        JobClient.runJob(conf);
    }
}
