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


/**
 * Created by qianzhang on 11/7/16.
 */
public class DataDividerByUser{
    public static class DataDividerMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
            //value is in the form: userID,MovieId,MovieRating
            String[] line = value.toString().trim().split(",");
            //information missing, just return

            //outputKey is userID
            String outputKey = line[0];
            //outputValue is movieID:MovieRating
            String outputValue = line[1] + ":" + line[2];
            context.write(new Text(outputKey), new Text(outputValue));
        }

    }

    public static class DataDividerReduce extends Reducer<Text, Text, Text, Text> {
        @Override

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            if(key == null || key.toString().trim().length() == 0) {
                return;
            }
                //values: {1:8, 2:3, 3:0, 4:9}
                //outputValue need: 1:8#2:3#3:0#4:9
                //outputKey: userId
                //output will look like a row vector userId | movie1:movie1Rating | movie2:movie2Rating | movie3:movie3Rating...
                for(Text temp : values) {
                    sb.append(temp + "#");
                }

            sb = sb.deleteCharAt(sb.length() - 1);
            String outputValue = sb.toString();
            context.write(key, new Text(outputValue));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReduce.class);
        job.setJarByClass(DataDividerByUser.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
