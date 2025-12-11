// DailyRevenue.java
import java.io.IOException;
import java.time.Instant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DailyRevenue {
    private static final String INPUT_PATH = "input-q3/";
    private static final String OUTPUT_PATH = "output/daily-";
    private static final String CSV_SPLIT = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.toLowerCase().contains("conversion_id")) return;
            String[] f = line.split(CSV_SPLIT, -1);
            if (f.length <= 10) return;
            String dateId = f[2].replaceAll("^\"|\"$", "").trim();
            String eventType = f[8].replaceAll("^\"|\"$", "").trim();
            // we consider only PURCHASE events
            if (!"PURCHASE".equalsIgnoreCase(eventType)) return;
            String convValueStr = f[9].replaceAll("^\"|\"$", "").trim();
            String convCountStr = f[10].replaceAll("^\"|\"$", "").trim();
            if (dateId.isEmpty()) return;
            // default to 0 if parse fails
            double val = 0.0;
            long cnt = 0L;
            try { val = Double.parseDouble(convValueStr); } catch (Exception e) { val = 0.0; }
            try { cnt = Long.parseLong(convCountStr); } catch (Exception e) { cnt = 0L; }
            context.write(new Text(dateId), new Text(val + "|" + cnt));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalRevenue = 0.0;
            long totalConversions = 0L;
            for (Text t : values) {
                String[] p = t.toString().split("\\|", -1);
                double v = 0.0; long c = 0;
                try { v = Double.parseDouble(p[0]); } catch (Exception e) { v = 0.0; }
                try { c = Long.parseLong(p[1]); } catch (Exception e) { c = 0L; }
                totalRevenue += v;
                totalConversions += c;
            }
            double avg = totalConversions > 0 ? (totalRevenue / totalConversions) : 0.0;
            String outVal = totalRevenue + "," + totalConversions + "," + avg;
            context.write(key, new Text(outVal));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DailyRevenue");
        job.setJarByClass(DailyRevenue.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input_datamart/fact_conversion.csv"));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
