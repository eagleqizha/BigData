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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;


public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<LongWritable, Text, Text, Text> {


        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            //read line by line
                String line = value.toString().trim();
                String[] relation = line.split("\t");
                //if there is only one column, do nothing but return
                if (relation.length == 1 || relation[1].trim().equals("")) {
                    return;
                }
                //outputKey: 'from' edge
                String outputKey = relation[0];
                //outputValues: 'to' edges
                String[] outputValues = relation[1].split(",");

                for (String val : outputValues) {
                    String outValue = val + "=" + (double) 1 / outputValues.length;
                    //write: 'to' edge = its probability
                    context.write(new Text(outputKey), new Text(outValue));
                }

        }
    }
    public static class PRMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //outputKey:  edge
            //outputValue: the probability to this edge
            String[] pairs = value.toString().trim().split("\t");
            String outputKey = pairs[0];
            String outputValue = pairs[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }


    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> trans = new ArrayList<String>();
            double pr0 = 0;
            for (Text value : values) {

                if (value.toString().contains("=")) {
                    //value from transition matrix
                    trans.add(value.toString());
                } else {
                    //value from pagerank vector
                    pr0 = Double.parseDouble(value.toString());
                }
            }

            for (String s : trans) {
                String outputKey = s.split("=")[0];
                //multiply cell by cell, pass data to unitSum mapper and sum it up in reducer. 
                double outputValue = Double.parseDouble(s.split("=")[1]) * pr0;   
                context.write(new Text(outputKey), new Text(String.valueOf(outputValue)));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);
        job.setMapperClass(TransitionMapper.class);

        job.setReducerClass(MultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //two mappers reading two files. don't need chainmapper
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
    
}
