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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatusCode {

    // Mapper class
    public static class StatusCodeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text statusCode = new Text();

        // Regex to extract status code from request
        private final static Pattern pattern = Pattern.compile("\"\\w+\\s[^\\s]+\\sHTTP/\\d\\.\\d\"\\s(\\d{3})");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            if (matcher.find()) {
                statusCode.set(matcher.group(1)); // e.g., "200", "404"
                context.write(statusCode, one);
            }
        }
    }

    // Reducer class
    public static class StatusCodeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text code, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable count : counts) {
                total += count.get();
            }
            context.write(code, new IntWritable(total));
        }
    }

    // Main method (Driver)
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: StatusCodeAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HTTP Status Code Count");

        job.setJarByClass(StatusCode.class);
        job.setMapperClass(StatusCodeMapper.class);
        job.setReducerClass(StatusCodeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
