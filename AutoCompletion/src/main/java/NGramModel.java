
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by qianzhang on 9/26/16.
 */
public class NGramModel {


    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        int limit;
        //get N for "N"GRam
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            limit = conf.getInt("NGram", 5);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        //put change value to lowercase and replace non-character by space
            String line = value.toString().trim().toLowerCase();
            line = line.replaceAll("[^a-z]]", " ");

            //split sentence into words
            String[] values = line.trim().split("\\s+");
            //1Gram not needed
            if(values.length < 2) {
                return;
            }
            StringBuilder sb;
            //getting NGram and write output
            for(int i = 0; i < values.length - 1; i++) {
                sb = new StringBuilder();
                sb.append(values[i]);
                for(int j = 1; j < limit && i + j < values.length; j++) {
                    sb.append(" "). append(values[i + j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }

        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            //counting total count for key
            for(IntWritable value : values) {
                sum ++;
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
