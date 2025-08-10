import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.*;

public class ProductCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Pattern pattern = Pattern.compile("GET\\s+/(m/)?product/(\\d+)");
    private final static IntWritable one = new IntWritable(1);
    private Text productId = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = pattern.matcher(value.toString());
        if (matcher.find()) {
            productId.set(matcher.group(2));
            context.write(productId, one);
        }
    }
}