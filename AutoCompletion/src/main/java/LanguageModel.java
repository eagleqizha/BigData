
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Created by qianzhang on 9/26/16.
 */
public class LanguageModel {

    public static class LangModelMapper extends Mapper<LongWritable, Text, Text, Text> {
        int threshold;
        @Override
        //setup threshold
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            threshold = conf.getInt("threshold", 20);

        }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            //check boundary
            if(value == null || value.toString().trim().length() == 0) {
                return;
            }

            String line = value.toString().trim();
            //split previous output into words + count

            String[] wordsPlusCount = line.split("\t");

            if(wordsPlusCount.length < 2) {
                return;
            }

            int count = Integer.parseInt(wordsPlusCount[1]);
            //optimize program by ignore NGram which has count > threshold
            if(count < threshold){
                return;
            }

            //split words to get the N-1 Gram and last word
            String[] words = wordsPlusCount[0].split("\\s+");
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]).append(" ");

            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1];

            if(! (outputKey == null || outputKey.length() < 1)) {
                context.write(new Text(outputKey), new Text(outputValue + "=" + count));
            }
            
        }
    }

    public static class LangModelReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        int limit;
        @Override
        //get limit for MySQL
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            limit = conf.getInt("SQL", 5);
        }
        private class WordCount{
            String word;
            int count;
            public WordCount(String word, int count){
                this.word = word;
                this.count = count;
            }
        }
        private Comparator<WordCount>  WordComparator = new Comparator<WordCount>(){
            public int compare(WordCount w1, WordCount w2) {
                if(w1.count > w2.count) {
                    return -1;
                }else if(w1.count < w2.count) {
                    return 1;
                }else {
                    return 0;
                }
            }
        };

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	        if(key == null || values == null) {
		        return ;
	        }
            //I love --> <big = 1002, cat ==5000, dog = 2311, tiger...>
            Queue<WordCount> heap = new PriorityQueue<WordCount>(limit, WordComparator);
            for(Text value : values) {
                //split each value in values to word+count to create WordCount class and offer it to priorityqueue in descending order
                String word = value.toString().split("=")[0].trim();
                int count = Integer.parseInt(value.toString().split("=")[1].trim());
                WordCount wc = new WordCount(word, count);

            }
            int i = 0;
            //poll the element and write to database.
            while(!heap.isEmpty() && i < limit) {
                context.write(new DBOutputWritable(key.toString(), heap.poll().word, heap.poll().count), NullWritable.get());
                i++;
            }
        }

    }
}
