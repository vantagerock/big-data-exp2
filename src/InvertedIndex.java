import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class InvertedIndex {

    // final static String INPUT="hdfs://master001:9000/data/exp2";
    // final static String OUTPUT="hdfs://master001:9000/2024stu1_22_out/exp2";

    public static void main(String[] args) throws Exception{
        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration,"Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(MyMap.class);
        job.setCombinerClass(MyCombine.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(job,new Path(INPUT));
        FileInputFormat.addInputPath(job,new Path(args[0]));
        // FileOutputFormat.setOutputPath(job,new Path(OUTPUT));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}