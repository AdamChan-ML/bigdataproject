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

public class DailyTraffic {

    public static class DateMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text dateKey = new Text();

        // Regex to extract date from log line: [10/Oct/2000:13:55:36 -0700]
        private static final Pattern datePattern = Pattern.compile("\\[(\\d{2}/[A-Za-z]{3}/\\d{4}):");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = datePattern.matcher(line);
            if (matcher.find()) {
                String date = matcher.group(1); // Extract only the date part
                dateKey.set(date);
                context.write(dateKey, one);
            }
        }
    }

    public static class DateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable total = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            total.set(count);
            context.write(key, total);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Daily Web Traffic");
        job.setJarByClass(DailyTraffic.class);
        job.setMapperClass(DateMapper.class);
        job.setCombinerClass(DateReducer.class);
        job.setReducerClass(DateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input directory
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
