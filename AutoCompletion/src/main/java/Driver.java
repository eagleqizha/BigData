import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by qianzhang on 9/26/16.
 */
public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("NGram", args[0]);
        conf.set("textinputformat.record.delimiter", ".");

        // setup first mapreduce job
        Job job1 = Job.getInstance();
        job1.setMapperClass(NGramModel.NGramMapper.class);
        job1.setReducerClass(NGramModel.NGramReducer.class);
        job1.setJarByClass(Driver.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(args[1]));
        TextOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        //after first job completed, start second job.
        Configuration conf2 = new Configuration();
        conf2.set("threshold", args[3]);
        conf2.set("SQL", args[4]);
        DBConfiguration.configureDB(conf2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.1.8:3306/test",
                "root",
                "ac870125");
        Job job2 = Job.getInstance(conf2);
        job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));
        job2.setJarByClass(Driver.class);
	job2.setMapperClass(LanguageModel.LangModelMapper.class);
        job2.setReducerClass(LanguageModel.LangModelReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        TextInputFormat.setInputPaths(job2, new Path(args[2]));
        DBOutputFormat.setOutput(job2, "Output", new String[] {"starting_word", "following_word", "count"});

        job2.waitForCompletion(true);



    }

}
