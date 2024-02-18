import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class part3P5 {

    public static class IPMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text ipAddress = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\\s+");
            if (parts.length > 0) {
                ipAddress.set(parts[0]);
                context.write(ipAddress, one);
            }
        }
    }

    public static class IPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text mostAccessedIP = new Text();
        private IntWritable maxAccesses = new IntWritable(Integer.MIN_VALUE);

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            if (sum > maxAccesses.get()) {
                mostAccessedIP.set(key);
                maxAccesses.set(sum);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(mostAccessedIP.toString() + "  "), maxAccesses);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Accessed IP");
        job.setJarByClass(part3P5.class);
        job.setMapperClass(IPMapper.class);
        job.setCombinerClass(IPReducer.class);
        job.setReducerClass(IPReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
