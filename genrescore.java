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

public class genrescore {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text genre = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("genre")) {
                return;
            }

            String[] fields = value.toString().split("\t");
            
            if (fields.length >= 6 && !fields[2].isEmpty()) {
                try {
                    String migenre = fields[2];
                    genre.set(migenre);
                    float score = Float.parseFloat(fields[5]);
                    context.write(genre, new FloatWritable(score));
                } catch (NumberFormatException e) {
                    // if the score field is not a number, ignore this line
                }
            }
        }
    }

    public static class FloatAvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count >= 10) {
                result.set(sum / count);
                System.out.println("Reducer Output=> Genre: " + key + ", Average Score: " + result);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "genrescore");
        job.setJarByClass(genrescore.class);
        job.setMapperClass(TokenizerMapper.class);
        // remove combiner as it's not applicable for averaging
        // job.setCombinerClass(FloatAvgReducer.class);
        job.setReducerClass(FloatAvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
