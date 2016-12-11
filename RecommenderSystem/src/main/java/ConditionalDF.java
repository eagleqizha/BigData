import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by qianzhang on 11/7/16.
 */
public class ConditionalDF {

    public static class ConditionMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        //input format is movieA:movieB"\t"count
        //output format is outputKey = movieA, outputValue = movieB=count
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if(line.length < 2) {
                return;
            }
            String movies =  line[0];
            String count = line[1];
            String[] movieAplusB = movies.split(":");
            if(movieAplusB.length < 2) {
                return;
            }
            String movieA = movieAplusB[0];
            String movieB = movieAplusB[1];
            context.write(new Text(movieA), new Text(movieB + "=" + count));

        }
    }
    public static class ConditionReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        //find the conditaional probability, transpose the co-occurrence matrix
        //input is key: movieA, value: movieB=count
        //output is key: movieB, value: movieA=count/sum;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(key == null || key.toString().trim().length() == 0) {
                return;
            }
            int sum = 0;
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            for(Text temp : values) {
                String[] moviePlusCount = temp.toString().split("=");
                if(moviePlusCount.length < 2) {
                    return;
                }
                String movieB = moviePlusCount[0];

                int count = Integer.parseInt(moviePlusCount[1]);

                map.put(movieB, count);
                sum += count;
            }
            for(String temp : map.keySet()) {
                context.write(new Text(temp), new Text(key.toString() + "=" + map.get(temp) * 1.0 / sum ));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(ConditionMapper.class);
        job.setReducerClass(ConditionReducer.class);
        job.setJarByClass(ConditionalDF.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
