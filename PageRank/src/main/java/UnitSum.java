/**
 * Created by qianzhang on 10/6/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {

    public static class PassMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        float beta;
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String pageNum = value.toString().split("\t")[0];
            double prob = Double.parseDouble(value.toString().split("\t")[1]) * (1 - beta);
            context.write(new Text(pageNum), new DoubleWritable(prob));
        }
    }

    public static class SpiderMapper extends Mapper<LongWritable, Text, Text,DoubleWritable> {
        float beta;
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //adding the belta value to the calculation
            String[] prvalues = value.toString().trim().split("\t");
            String outputKey = prvalues[0];
            double pr0 = Double.parseDouble(prvalues[1]) * beta;
            context.write(new Text(outputKey), new DoubleWritable(pr0));
        }

    }
    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for(DoubleWritable value : values) {
               sum += value.get();
            }
            DecimalFormat dfm = new DecimalFormat("#.0000");
            context.write(key, new DoubleWritable(Double.parseDouble(dfm.format(sum))));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //two mappers to read two files.
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SpiderMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
