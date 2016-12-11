
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by qianzhang on 11/7/16.
 */
public class CoOccurrenceMatrixGenerator {
    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            //value is in the form: userId"\t"movie1:movie1Rating#movie2:movie2Rating#movie3:movie3Rating#movie4:movie4Rating
            //DataDividerByUser's reducer's result is in the form :outputValue"\t"outputValue
            String[] userWithRating = line.split("\t");
            if(userWithRating.length != 2) {
                return;
            }
            String[] movieRating = userWithRating[1].split("#");

            for(int i = 0; i < movieRating.length; i++) {
                //get movie1
                String movie1 = movieRating[i].trim().split(":")[0];
                for(int j = 0; j < movieRating.length; j++) {
                    //for movie1, find all the related movie
                    String movie2 = movieRating[j].trim().split(":")[0];
                    context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
                }
            }

        }
    }

    public static class MatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        /*find count for each two related movies, need normalization(actually, find conditional distribution)
          in the next step.*/
        public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            if(key == null || key.toString().trim().length() == 0) {
                return;
            }
            for(IntWritable temp : values) {
                sum++;
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setJarByClass(CoOccurrenceMatrixGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);


        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}















































































