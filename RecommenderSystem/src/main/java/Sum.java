import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by qianzhang on 11/7/16.
 */
public class Sum {
    public static class SumMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        //intput: userID:rating"\t"suggestedRating
        //output: key = userID:rating, value = suggestedRating
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\t");
            if(line.length < 2) {
                return;
            }
            String outputKey = line[0];
            String outputValue = line[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }

    }

    public static class SumReduer extends Reducer<Text, Text, Text, Text> {
        @Override
        //Sum up all the suggested ratings for the same key
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(Text value: values) {
                sum += Double.parseDouble(value.toString());
            }
            DecimalFormat dfmt = new DecimalFormat("0.00");
            String outputValue = dfmt.format(sum);
            context.write(key, new Text(outputValue));
        }

    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReduer.class);
        job.setJarByClass(Sum.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
