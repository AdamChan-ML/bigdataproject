import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HourTraffic {

    public static class HourMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text hourText = new Text();

        // Regex to match timestamp like: [10/Oct/2000:13:55:36 -0700]
        private static final Pattern logPattern = Pattern.compile("\\[(\\d{2}/[A-Za-z]{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = logPattern.matcher(line);
            if (matcher.find()) {
                String hour = matcher.group(2); // Extract the hour
                hourText.set(hour);
                context.write(hourText, one);
            }
        }
    }

    public static class HourReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
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
        Job job = Job.getInstance(conf, "Hourly Web Traffic");
        job.setJarByClass(HourTraffic.class);
        job.setMapperClass(HourMapper.class);
        job.setCombinerClass(HourReducer.class);  // Optional: Use reducer as combiner
        job.setReducerClass(HourReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and output paths from args
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
