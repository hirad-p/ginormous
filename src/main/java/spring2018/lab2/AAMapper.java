package spring2018.lab2;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.ArrayList;

public class AAMapper extends Mapper <LongWritable,Text,Text,Text> {

    Map<String, String> codon2aaMap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            Path[] codon2aaPath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(codon2aaPath != null && codon2aaPath.length > 0) {
                codon2aaMap = readFile(codon2aaPath);
            }
            } catch(IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
                System.exit(1);
            }
        }

    protected HashMap<String, String> readFile(Path[] codonFilePath) {
        HashMap<String, String> codonMap = new HashMap<String, String>();
        BufferedReader cacheReader=null;
        String line=null;
        String[] lineArray=null;
        try{
           cacheReader = new BufferedReader(new FileReader(codonFilePath[0].toString()));
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

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
        
    		String valueStr = value.toString();

    		ArrayList<String> frame1 = splitByFrame(valueStr, 1);
        ArrayList<String> frame2 = splitByFrame(valueStr, 2);
        ArrayList<String> frame3 = splitByFrame(valueStr, 3);

        // TCA GCC TTT TCT TTG ACC TCT TCT TTC TGT TCA TGT GTA TTT GCT GTC TCT TAG CCC AGA
        // does TCA exist in codon2aaMap?
        // if so, write (key, value) pair to context
        for (String str: frame1) {
            if (codon2aaMap.containsKey(str)) {
                context.write(new Text(codon2aaMap.get(str)), new Text("1"));
            }
        }

        // T CAG CCT TTT CTT TGA CCT CTT CTT TCT GTT CAT GTG TAT TTG CTG TCT CTT AGC CCA GA
        // does CAG exist in codon2aaMap?
        // if so, write (key, value) pair to context 
        for (String str: frame2) {
            if (codon2aaMap.containsKey(str)) {
                context.write(new Text(codon2aaMap.get(str)), new Text("2"));
            }
        }

        // TC AGC CTT TTC TTT GAC CTC TTC TTT CTG TTC ATG TGT ATT TGC TGT CTC TTA GCC CAG A
        // does AGC exist in codon2aaMap?
        // if so, write (key, value) pair to context 
        for (String str: frame3) {
            if (codon2aaMap.containsKey(str)) {
                context.write(new Text(codon2aaMap.get(str)), new Text("3"));
            }
        }
    }

    public ArrayList<String> splitByFrame(String line, int frame) {
        ArrayList<String> result = new ArrayList<>();
        String text;

        if (frame == 1) {
            text = line;
        } else if (frame == 2) {
            result.add(line.substring(0, 1));
            text = line.substring(1);
        } else if (frame == 3) {
            result.add(line.substring(0, 2));
            text = line.substring(2);
        } else {
            throw new Error("Invalid Frame type");
        }

        for (int i = 0; i < text.length(); i += 3)  {
            result.add(text.substring(i, Math.min(i + 3, text.length())));
        }

        return result;
    }
}