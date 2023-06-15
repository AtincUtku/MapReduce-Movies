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

public class ratescore {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header row
            if (key.get() == 0 && value.toString().contains("rating")) {
                return;
            }

            String[] fields = value.toString().split("\t");
            
            // assuming rating is in the 5th field and votes in the 6th
            if (fields.length >= 6) {
                try {
                    String rating = fields[1];
                    float votes = Float.parseFloat(fields[6]);
                    if ("G".equals(rating) || "PG".equals(rating) || "PG-13".equals(rating) || "R".equals(rating)) {
                        word.set(rating);
                        context.write(word, new FloatWritable(votes));
                    }
                } catch (NumberFormatException e) {
                    // if the votes field is not a number, ignore this line
                }
            }
        }
    }

    public static class FloatAvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ratescore");
        job.setJarByClass(ratescore.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(FloatAvgReducer.class);
        job.setReducerClass(FloatAvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
