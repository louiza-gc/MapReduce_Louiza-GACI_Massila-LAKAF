
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupBy {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/groupBy-";
	private static final Logger LOG = Logger.getLogger(GroupBy.class.getName());

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

		try {
			FileHandler fh = new FileHandler("out.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		} catch (SecurityException | IOException e) {
			System.exit(1);
		}
	}


	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{


            String line = value.toString();
            String[] words = line.split(",");

            // IGNORER la ligne des noms de colonnes
            if(words[0].equals("Row ID")){
                return;
            }

            /*
            ** Pour Exercice 2
            double valueKey = Double.parseDouble(words[20]);

            context.write(new Text(words[5]), new DoubleWritable(valueKey));

            LOG.info(words[5]+"   "+new DoubleWritable(valueKey));
             */



            // Modifier pour Exercice 3 =====> Calculer les ventes par Date et State.
            /*
            String orderDate = words[2];
            String state = words[10];
            String compositeKey = orderDate + "\t" + state;

            // certaines valeurs de "Sales" ont des caracteres non numeriques ( 16GB par ex), on doit ignorer ca

            double valueKey;
            try {
                valueKey = Double.parseDouble(words[17]);   // <-- Problème ici si "16GB"
                context.write(new Text(compositeKey), new DoubleWritable(valueKey));
                LOG.info(compositeKey + "    " + valueKey);
            } catch (NumberFormatException e) {
                // Ignorer les lignes non numériques
                return;
            }
            */



            // Modifier pour Exercice 3 =====> Calculer les ventes par Date et Category.
            String orderDate = words[2];
            String category = words[14];
            String compositeKey = orderDate + "\t" + category;

            // certaines valeurs de "Sales" ont des caracteres non numeriques ( 16GB par ex), on doit ignorer ca

            double valueKey;
            try {
                valueKey = Double.parseDouble(words[17]);   // <-- Problème ici si "16GB"
                context.write(new Text(compositeKey), new DoubleWritable(valueKey));
                LOG.info(compositeKey + "    " + valueKey);
            } catch (NumberFormatException e) {
                // Ignorer les lignes non numériques
                return;
            }
		}

    }



    /*
    Calculer par command:
      -Le nombre de produits distincts achetés.
      -Le nombre total d'exemplaires.
     */
    // on change le Map
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{


            String line = value.toString();
            String[] words = line.split(",");

            // Ignorer le header
            if (words[0].equals("Row ID")) return;

            String orderId = words[1];    // Order ID
            String productId = words[13];  // Product ID
            String quantityStr = words[18];// Quantity

            // Vérification numérique
            int quantity;
            try {
                quantity = Integer.parseInt(quantityStr);
            } catch (NumberFormatException e) {
                return; // ignore invalid quantity
            }

            context.write(new Text(orderId), new Text(productId + ":" + quantity));
        }
    }

    /*
    Calculer par command:
      -Le nombre de produits distincts achetés.
      -Le nombre total d'exemplaires.
    */
    // on change le Reduce aussi
    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // on initialise un HashSet pour pouvoir rajouter les elements tout en evitant les doublons
            Set<String> uniqueProducts = new HashSet<>();
            int totalQuantity = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String productId = parts[0];
                int qty = Integer.parseInt(parts[1]);

                uniqueProducts.add(productId);
                totalQuantity += qty;
            }

            String result = "DistinctProducts=" + uniqueProducts.size() +
                    "\tTotalQuantity=" + totalQuantity;

            context.write(key, new Text(result));
        }
    }





    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
            // Exercice 2

            double sum = 0.0;

            for (DoubleWritable val : values)
                sum += val.get();

            context.write(key, new DoubleWritable(sum));
        }
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "GroupBy");

		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);

		//job.setMapperClass(Map.class);
		//job.setReducerClass(Reduce.class);

        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class); // mapper émet Text

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); // reducer émet IntWritable

		//job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}