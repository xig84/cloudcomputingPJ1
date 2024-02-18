import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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

public class part3P3 {

    public static class HTTPMethodsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\"");
            if (parts.length >= 3) {
                String[] requestParts = parts[1].split(" ");
                if (requestParts.length >= 1) {
                    String method = requestParts[0];
                    if (isValidMethod(method)) { 
                         context.write(new Text(method), one);
                        }
                    
                }
            }
        }

   private boolean isValidMethod(String method) {
        return method.equals("GET") || method.equals("HEAD") || method.equals("POST") ||
               method.equals("PUT") || method.equals("DELETE") || method.equals("TRACE") ||
               method.equals("OPTIONS") || method.equals("PROPFIND") || method.equals("SEARCH") ||
               method.equals("TRACK");
    }
    }

    public static class HTTPMethodsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
               sum += value.get();
           }
           context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HTTP Methods");
        job.setJarByClass(part3P3.class);
        job.setMapperClass(HTTPMethodsMapper.class);
        job.setCombinerClass(HTTPMethodsReducer.class);
        job.setReducerClass(HTTPMethodsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
