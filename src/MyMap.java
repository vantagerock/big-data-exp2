import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMap extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit=(FileSplit) context.getInputSplit();
        String filename=fileSplit.getPath().getName();
        String line=value.toString().toLowerCase().replaceAll("[^a-zA-Z]"," ");
        StringTokenizer itr=new StringTokenizer(line);
        while (itr.hasMoreTokens()){
            Text ketOut=new Text(itr.nextToken()+":"+filename);
            Text valueOut=new Text("1");
            context.write(ketOut,valueOut);
        }
    }
}