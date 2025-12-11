import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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

public class Join {
    private static final String INPUT_PATH = "input-join/";
	private static final String OUTPUT_PATH = "output/join-";

    private static final int CUST_ID_IDX_CUSTOMERS = 0;
    private static final int CUST_NAME_IDX_CUSTOMERS = 1;

    private static final int CUST_ID_IDX_ORDERS = 1;
    private static final int ORDER_COMMENT_IDX_ORDERS = 8;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private String filename;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            filename = split.getPath().getName().toLowerCase();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] fields = line.split("\\|", -1);

            if (filename.contains("customers")) {
                // customers.tbl : emit (CustomerID, "C|CustomerName")
                String custId = fields[CUST_ID_IDX_CUSTOMERS];
                String name = fields[CUST_NAME_IDX_CUSTOMERS];
                context.write(new Text(custId), new Text("C|" + name));
            } else if (filename.contains("orders")) {
                // orders.tbl : emit (CustomerID, "O|OrderComment")
                String custId = fields[CUST_ID_IDX_ORDERS];
                String comment = fields[ORDER_COMMENT_IDX_ORDERS];
                context.write(new Text(custId), new Text("O|" + comment));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> customers = new ArrayList<>();
            List<String> orders = new ArrayList<>();

            // copier les valeurs dans des tableaux temporaires
            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("C|")) {
                    customers.add(s.substring(2));
                } else if (s.startsWith("O|")) {
                    orders.add(s.substring(2));
                }
            }

            // deux boucles imbriquées : pour chaque customer.name x chaque order.comment
            for (String custName : customers) {
                for (String orderComment : orders) {
                    // output: (CUSTOMERS.name, ORDERS.comment)
                    context.write(new Text(custName), new Text(orderComment));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Join");
		job.setJarByClass(Join.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}
