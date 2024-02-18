import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NGramFrequency {

    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text ngram = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n"));
            String line = value.toString();
            line = line.replaceAll("[^a-zA-Z0-9\\s]", ""); // Remove non-alphanumeric characters
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (word.length() >= n) {
                    for (int i = 0; i <= word.length() - n; i++) {
                        ngram.set(word.substring(i, i + n));
                        context.write(ngram, one);
                    }
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("n", args[2]); // Set n value from command line arguments

        Job job = Job.getInstance(conf, "n-gram frequency");
        job.setJarByClass(NGramFrequency.class);
        job.setMapperClass(NGramMapper.class);
        job.setCombinerClass(NGramReducer.class);
        job.setReducerClass(NGramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
