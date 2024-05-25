import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MyCombine extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key -> word:filename
        // values -> 1,1,...
        int num=0;
        for (Text value:values){
            num+=Integer.parseInt(value.toString());
        }
        int index=key.toString().indexOf(':');
        Text keyOut=new Text(key.toString().substring(0,index));
        Text valueOut=new Text(key.toString().substring(index+1)+":"+String.valueOf(num));
        // keyOut -> word
        // valueOut -> filename:num
        context.write(keyOut,valueOut);
    }
}