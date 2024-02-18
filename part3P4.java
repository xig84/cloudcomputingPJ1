import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

public class part3P4 {

    public static class PathMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\"");
            if (parts.length >= 3) {
                String[] requestParts = parts[1].split(" ");
                if (requestParts.length >= 2) {
                    String path = requestParts[1];
                    context.write(new Text(path), one);
                }
            }
        }
    }

    public static class PathReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<String, Integer> pathCounts = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            pathCounts.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String mostVisitedPath = "";
            int maxCount = 0;
            for (Map.Entry<String, Integer> entry : pathCounts.entrySet()) {
                String path = entry.getKey();
                int count = entry.getValue();
                if (count > maxCount) {
                    mostVisitedPath = path;
                    maxCount = count;
                }
            }
            context.write(new Text(mostVisitedPath), new IntWritable(maxCount));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Visited Path");
        job.setJarByClass(part3P4.class);
        job.setMapperClass(PathMapper.class);
        job.setReducerClass(PathReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
