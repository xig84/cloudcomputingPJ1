import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class part3P1 {

    public static class WebsiteDirectoryHitsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final String DIRECTORY = "/images/smilies/";

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\"");
            if (parts.length >= 3) {
                String[] requestParts = parts[1].split(" ");
                if (requestParts.length >= 2) {
                    String requestPath = requestParts[1];
                    if (requestPath.startsWith(DIRECTORY)) {
                        context.write(new Text(requestPath), new IntWritable(1));
                    }
                }
            }
        }
    }

public static class WebsiteDirectoryHitsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Text directoryKey = new Text("/images/smilies/");

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
      
        context.write(directoryKey, new IntWritable(sum));
    }
}


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Website Directory Hits");
        job.setJarByClass(part3P1.class);
        job.setMapperClass(WebsiteDirectoryHitsMapper.class);
        job.setCombinerClass(WebsiteDirectoryHitsReducer.class);
        job.setReducerClass(WebsiteDirectoryHitsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
