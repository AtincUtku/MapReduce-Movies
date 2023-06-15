import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class average {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private final static FloatWritable ONE = new FloatWritable(1.0f);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("Map method called with value: " + value);

            // Skip the header row
            if (key.get() == 0 && value.toString().contains("runtime")) {
                System.out.println("Skipping header row");
                return;
            }

            String[] fields = value.toString().split("\t");
            if (fields.length == 15) {
                try {
                    float runtime = Float.parseFloat(fields[14]);  // runtime is the last field
                    word.set("average_runtime");
                    context.write(word, new FloatWritable(runtime));
                    System.out.println("Mapped " + runtime + " to average_runtime");
                } catch (NumberFormatException e) {
                    System.out.println("NumberFormatException, skipping line");
                    // if the last field is not a number, ignore this line
                }
            } else {
                System.out.println("Incorrect number of fields, skipping line");
            }
        }
    }

    public static class FloatAvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("Reduce method called with key: " + key);

            float sum = 0.0f;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float average = sum / count;
            result.set(average);
            context.write(key, result);
            System.out.println("Wrote average: " + average);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average_runtime");
        job.setJarByClass(average.class);
        job.setMapperClass(TokenizerMapper.class);
        // Removed combiner here because it may lead to incorrect results for averages
        job.setReducerClass(FloatAvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
