package spring2018.lab2;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class AAReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Long frame1Count = 0L;
        Long frame2Count = 0L;
        Long frame3Count = 0L;

        int frame = 0;
        for (Text value : values) {
            frame = Integer.parseInt(value.toString());
            if (frame == 1) {
                frame1Count++;
            } else if (frame == 2) {
                frame2Count++;
            } else if (frame == 3) {
                frame3Count++;
            }
        }
       
        String spaces = "";
        for (int i = key.getLength(); i < 8; i++) {
        		spaces += " ";
        }
        Text newKey = new Text(key.toString() + spaces);
        Text summary =
                new Text(frame1Count.toString() + "\t" +  frame2Count.toString() + "\t" + frame3Count.toString());
        context.write(newKey, summary);
    }
}