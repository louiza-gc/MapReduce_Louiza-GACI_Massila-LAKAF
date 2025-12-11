import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

public class MerchantSummary {
    private static final String INPUT_DIR = "input_datamart/";
    private static final String OUTPUT_DIR = "output/merchantsummary-";
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
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            // skip header lines
            String low = line.toLowerCase();
            if (low.contains("merchant_id") && (filename.contains("snapshot") || filename.contains("snapshot_merchant"))) return;
            if (low.contains("merchant_id") && filename.contains("dim_merchant")) return;

            String[] f = line.split(CSV_SPLIT, -1);

            if (filename.contains("snapshot_merchant") || filename.contains("snapshot")) {
                if (f.length < 5) return;
                String mid = f[0].replaceAll("^\"|\"$", "").trim();
                String totalRevenue = f[2].replaceAll("^\"|\"$", "").trim();
                String uniqueCustomers = f[4].replaceAll("^\"|\"$", "").trim();
                if (mid.isEmpty()) return;
                // tag S for snapshot
                context.write(new Text(mid), new Text("S|" + totalRevenue + "|" + uniqueCustomers));
            } else if (filename.contains("dim_merchant") || filename.contains("merchant")) {
                if (f.length < 3) return;
                String mid = f[0].replaceAll("^\"|\"$", "").trim();
                String merchantName = f[2].replaceAll("^\"|\"$", "").trim();
                if (mid.isEmpty()) return;
                context.write(new Text(mid), new Text("D|" + merchantName));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String merchantName = null;
            String totalRevenue = "0";
            String uniqueCustomers = "0";

            List<String> snapshots = new ArrayList<>();
            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("D|")) {
                    merchantName = s.substring(2);
                } else if (s.startsWith("S|")) {
                    snapshots.add(s.substring(2));
                }
            }

            if (snapshots.isEmpty()) {
                // no snapshot row -> skip
                return;
            }

            // If multiple snapshot rows (shouldn't happen), sum unique/customers and revenue conservatively take sum
            double revSum = 0.0;
            long uniqSum = 0;
            for (String snap : snapshots) {
                String[] p = snap.split("\\|", -1);
                double r = 0.0;
                long u = 0;
                try { r = Double.parseDouble(p[0]); } catch (Exception e) { r = 0.0; }
                try { u = Long.parseLong(p[1]); } catch (Exception e) { u = 0L; }
                revSum += r;
                uniqSum += u;
            }
            totalRevenue = String.valueOf(revSum);
            uniqueCustomers = String.valueOf(uniqSum);

            // If merchantName missing, use merchant_id as fallback
            String outName = (merchantName != null && !merchantName.isEmpty()) ? merchantName : key.toString();
            // output: merchant_name,total_revenue,unique_customers
            context.write(new Text(outName), new Text(totalRevenue + "," + uniqueCustomers));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MerchantSummary");
        job.setJarByClass(MerchantSummary.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_DIR + "snapshot_merchant.csv"));
        FileInputFormat.addInputPath(job, new Path(INPUT_DIR + "dim_merchant.csv"));

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR + Instant.now().getEpochSecond()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
