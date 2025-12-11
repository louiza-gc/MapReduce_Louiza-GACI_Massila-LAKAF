import java.io.IOException;
import java.time.Instant;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MerchantJoin {
    private static final String INPUT_PATH = "input_datamart/";
    private static final String OUTPUT_PATH = "output/merchant-";
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
            if (line.toLowerCase().contains("conversion_id") || line.toLowerCase().contains("merchant_id")) {
                // skip headers
                return;
            }
            String[] f = line.split(CSV_SPLIT, -1);

            if (filename.contains("fact")) {
                // fact_conversion: merchant_id idx 6, conversion_value idx 9, conversion_count idx 10
                if (f.length <= 10) return;
                String merchantId = f[6].replaceAll("^\"|\"$", "").trim();
                String convValue = f[9].replaceAll("^\"|\"$", "").trim();
                String convCount = f[10].replaceAll("^\"|\"$", "").trim();
                if (merchantId.isEmpty()) return;
                context.write(new Text(merchantId), new Text("F|" + convValue + "|" + convCount));
            } else if (filename.contains("dim_merchant") || filename.contains("merchant")) {
                // dim_merchant: merchant_id idx0, merchant_name idx2, country idx4
                String[] parts = f;
                if (parts.length < 5) return;
                String mid = parts[0].replaceAll("^\"|\"$", "").trim();
                String name = parts[2].replaceAll("^\"|\"$", "").trim();
                String country = parts[4].replaceAll("^\"|\"$", "").trim();
                if (mid.isEmpty()) return;
                context.write(new Text(mid), new Text("M|" + country + "|" + name));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String country = null;
            String merchantName = null;
            double totalRevenue = 0.0;
            long totalConversions = 0L;

            List<String> facts = new ArrayList<>();
            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("M|")) {
                    String[] p = s.split("\\|", -1);
                    if (p.length >= 3) {
                        country = p[1];
                        merchantName = p[2];
                    }
                } else if (s.startsWith("F|")) {
                    facts.add(s.substring(2));
                }
            }

            if (country == null || merchantName == null) {
                // inner join behaviour: skip if merchant metadata missing
                return;
            }

            for (String f : facts) {
                String[] p = f.split("\\|", -1);
                double v = 0.0; long c = 0;
                try { v = Double.parseDouble(p[0]); } catch (Exception e) { v = 0.0; }
                try { c = Long.parseLong(p[1]); } catch (Exception e) { c = 0L; }
                totalRevenue += v;
                totalConversions += c;
            }

            String outKey = country + "," + merchantName;
            String outVal = String.valueOf(totalRevenue) + "," + String.valueOf(totalConversions);
            context.write(new Text(outKey), new Text(outVal));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MerchantJoin");
        job.setJarByClass(MerchantJoin.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH)); // contains fact_conversion.csv and dim_merchant.csv
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
