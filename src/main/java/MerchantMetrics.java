import java.io.IOException;
import java.time.Instant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MerchantMetrics {
    private static final String INPUT_DIR = "input_datamart/";
    private static final String OUTPUT_DIR = "output/merchantMetrics-";
    private static final String CSV_SPLIT = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private String filename;
        @Override
        protected void setup(Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            filename = split.getPath().getName().toLowerCase();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!(filename.contains("snapshot_merchant") || filename.contains("snapshot"))) return;
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            if (line.toLowerCase().contains("merchant_id")) return;
            String[] f = line.split(CSV_SPLIT, -1);
            if (f.length < 5) return;
            String merchantId = f[0].replaceAll("^\"|\"$", "").trim();
            double totalRevenue = 0.0;
            long eventsCount = 0;
            long purchaseCount = 0;
            long uniqueCustomers = 0;
            try { totalRevenue = Double.parseDouble(f[2].replaceAll("\"","").trim()); } catch (Exception e) { totalRevenue = 0.0; }
            try { eventsCount = Long.parseLong(f[1].replaceAll("\"","").trim()); } catch (Exception e) { eventsCount = 0; }
            try { purchaseCount = Long.parseLong(f[3].replaceAll("\"","").trim()); } catch (Exception e) { purchaseCount = 0; }
            try { uniqueCustomers = Long.parseLong(f[4].replaceAll("\"","").trim()); } catch (Exception e) { uniqueCustomers = 0; }

            double revenuePerCustomer = (uniqueCustomers > 0) ? (totalRevenue / uniqueCustomers) : 0.0;
            double purchaseRate = (eventsCount > 0) ? ((double)purchaseCount / (double)eventsCount) : 0.0;

            // emit merchant_id -> csv line with required metrics
            String out = merchantId + "," + revenuePerCustomer + "," + purchaseRate;
            context.write(new Text(merchantId), new Text(out));
        }
    }

    // Reducer just outputs the value (map-only could be used, but keep reducer for compatibility)
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                context.write(key, v); // v already contains merchant_id,... but key duplicates merchant_id; acceptable
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MerchantKPIsSimple");
        job.setJarByClass(MerchantMetrics.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_DIR + "snapshot_merchant.csv"));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR + Instant.now().getEpochSecond()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
