import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by qianzhang on 11/7/16.
 */
public class Multiplication {
    public static class CoMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        //output format: key=movieA, value=movieB=probability
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

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        //output format: key= movieA, value = use:rating
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split(",");
            if(line.length < 3) {
                return;
            }
            context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
        }
    }
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(key == null || key.toString().length() == 0) {
                return;
            }
            //check if current value is from CoMatrixMapper or RatingMapper
            HashMap<String, Double> probMap = new HashMap<String, Double>();
            HashMap<String, Double> ratingMap = new HashMap<String, Double>();
            for(Text temp : values) {
                String value = temp.toString().trim();
                if(value.contains(":")) {
                    ratingMap.put(value.split(":")[0], Double.parseDouble(value.split(":")[1]));
                }else if(value.contains("=")) {
                    probMap.put(value.split("=")[0], Double.parseDouble(value.split("=")[1]));
                }
            }
            for(Map.Entry<String, Double> entry: probMap.entrySet()) {
                String movieB = entry.getKey();
                double prob = entry.getValue();
                for(Map.Entry<String, Double> ratingValue: ratingMap.entrySet()) {
                    String userID = ratingValue.getKey();
                    double rating = ratingValue.getValue();
                    context.write(new Text(userID + ":" + movieB), new DoubleWritable(prob * rating));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setReducerClass(MultiplicationReducer.class);
        job.setJarByClass(Multiplication.class);


        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CoMatrixMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);

    }
}
