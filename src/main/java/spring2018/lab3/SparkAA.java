package spring2018.lab3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import scala.Tuple2;

import org.apache.hadoop.yarn.webapp.ResponseInfo.Item;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;

public class SparkAA {
    static Map<String, String> codon2aaMap = new HashMap<String, String>();

    public static void main(String[] args) throws Exception {
        // check args
        if(args.length != 3) {
            System.err.println("usage: SparkAA <input-file> <output-dir> <codon-table>");
            System.exit(1);
        }
        String inputFile = args[0];
        String outputDir = args[1];
        String codonTableFileName = args[2];
        
        // read in codon table into hash map
        codon2aaMap = readFile(codonTableFileName);

        // Create Java Spark Context
        SparkConf conf = new SparkConf().setAppName("sparkAA");
        SparkContext spark = new SparkContext(conf);
        
        // Load  input data
        JavaRDD<String> input = spark.textFile(inputFile, 1).toJavaRDD();
        
        // get counts for each reading frame
        JavaPairRDD<String, Integer> RF1aaCounts = getCounts(input, 0);
        JavaPairRDD<String, Integer> RF2aaCounts = getCounts(input, 1);
        JavaPairRDD<String, Integer> RF3aaCounts = getCounts(input, 2);
        
        // filter out the 0-counts -- output file should only have non-0 records
        JavaPairRDD<String, Integer> RF1nonzero = getNonZeroRDD(RF1aaCounts);
        RF1nonzero.saveAsTextFile(outputDir + "_RF1");

        JavaPairRDD<String, Integer> RF2nonzero = getNonZeroRDD(RF2aaCounts);
        RF2nonzero.saveAsTextFile(outputDir + "_RF2");

        JavaPairRDD<String, Integer> RF3nonzero = getNonZeroRDD(RF3aaCounts);
        RF3nonzero.saveAsTextFile(outputDir + "_RF3");
      
        spark.stop();
    }
    
    public static JavaPairRDD<String, Integer> getNonZeroRDD(JavaPairRDD<String, Integer> input) {
        JavaPairRDD<String, Integer> RFnonzero = input.filter(new Function<Tuple2<String, Integer>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, Integer> tuple) throws Exception {
				return tuple._2 != 0;
			}
        });
        return RFnonzero;
    }
    
    public static JavaPairRDD<String, Integer>  getCounts(JavaRDD<String> input, final int readingFrame) {
        JavaRDD<String> RFwords = input.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<String> call(String x) throws Exception {
				ArrayList<String> result = new ArrayList<>();
		        String text;
		        
		        switch (readingFrame) {
		        		case 0:	text = x;
		        				break;
		        		case 1: text = x.substring(1);
		        				break;
		        		case 2: text = x.substring(2);
		        				break;
		        		default:	throw new Error("Invalid Frame type");
		        }
		        
		        for (int i = 0; i < text.length(); i += 3)  {
		            result.add(text.substring(i, Math.min(i + 3, text.length())));
		        }
		        
		        return result.iterator(); 
			}
        });
        		  
        JavaPairRDD<String, Integer> RFcodonCounts = RFwords.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String x) throws Exception {
				Integer count = (codon2aaMap.containsKey(x)) ? 1 : 0;
				return new Tuple2<String, Integer>(codon2aaMap.get(x), count);	
			}
        });
        		        
        JavaPairRDD<String, Integer> RFaaCounts = RFcodonCounts.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
        
        return RFaaCounts;
    }
    
    
    
    protected static HashMap<String, String> readFile(String codonFilePath) {
        HashMap<String, String> codonMap = new HashMap<String, String>();
        BufferedReader cacheReader=null;
        String line=null;
        String[] lineArray=null;
        try{
           cacheReader = new BufferedReader(new FileReader(codonFilePath));
           while((line=cacheReader.readLine())!=null) {
               // Isoleucine      I       ATT, ATC, ATA
                 lineArray = line.split("\\t");
                 String aminoAcid = lineArray[0];
                 String[] sequencesArray = lineArray[2].split(",");
                 for(String sequence: sequencesArray) {
                     codonMap.put(sequence.trim(), aminoAcid.trim());
                 }
           }
        }
        catch(Exception e) { 
            e.printStackTrace(); 
            System.exit(1);
        }
        return codonMap;
    }
}
