import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TopIPReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text productId, Iterable<Text> ips, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> ipCount = new HashMap<>();

        for (Text ip : ips) {
            String ipStr = ip.toString();
            ipCount.put(ipStr, ipCount.getOrDefault(ipStr, 0) + 1);
        }

        List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(ipCount.entrySet());
        sortedList.sort((a, b) -> {
            int cmp = Integer.compare(b.getValue(), a.getValue());
            return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
        });

        int limit = Math.min(10, sortedList.size());
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, Integer> entry = sortedList.get(i);
            context.write(productId, new Text(entry.getKey() + "\t" + entry.getValue()));
        }
    }
}
