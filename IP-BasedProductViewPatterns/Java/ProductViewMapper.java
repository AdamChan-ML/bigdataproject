import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.*;

public class ProductViewMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern pattern = Pattern.compile("^(\\d+\\.\\d+\\.\\d+\\.\\d+).+GET\\s+/(m/)?product/(\\d+)");
    private static final Set<String> blacklist = new HashSet<>(Arrays.asList(
            "66.249.66.194", "91.99.72.15", "5.160.157.20"));

    private Text productId = new Text();
    private Text ip = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = pattern.matcher(value.toString());
        if (matcher.find()) {
            String ipAddr = matcher.group(1);
            if (blacklist.contains(ipAddr)) {
                return;
            }
            String prodId = matcher.group(3);
            productId.set(prodId);
            ip.set(ipAddr);
            context.write(productId, ip);
        }
    }
}