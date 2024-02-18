import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;


public class part3P9 {

    public static class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text ipAddress = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\\s+");
            if (parts.length > 0) {
                ipAddress.set(parts[0]);
                context.write(ipAddress, one);
            }
        }
    }

    public static class IPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, String> top3IPs = new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            top3IPs.put(sum, key.toString());
            if (top3IPs.size() > 3) {
                top3IPs.remove(top3IPs.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : top3IPs.descendingMap().entrySet()) {
	
		context.write(new Text(entry.getValue() + "   Bytes:"), new IntWritable(entry.getKey()));
    }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 3 IPs by Data Flow Size");
        job.setJarByClass(part3P9.class);
        job.setMapperClass(IPMapper.class);
        job.setReducerClass(IPReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
