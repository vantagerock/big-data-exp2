import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MyReduce extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // key -> word
        // values -> filename:num,filename:num...
        int nums=0;
        int count=0;
        StringBuilder files= new StringBuilder();
        for (Text value:values){
            int index=value.toString().indexOf(':');
            String num=value.toString().substring(index+1);
            nums+=Integer.parseInt(num);
            count++;
            files.append(value.toString());
            files.append(';');
        }
        double avg_num=(double)nums/(double)count;
        String str_avg=String.format("%.2f",avg_num);
        Text valueOut=new Text(str_avg+","+files.toString());
        context.write(key,valueOut);
    }
}