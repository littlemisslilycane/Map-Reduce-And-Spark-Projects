import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MaxFilter extends Configured implements Tool {

    public static class FilterMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());
            Configuration conf = context.getConfiguration();
            int max = Integer.parseInt(conf.get("MAX"));

            while (itr.hasMoreTokens()) {
                String[] inputValues = itr.nextToken().split(",");
                if (inputValues.length == 2) {
                    if (Integer.parseInt(inputValues[0]) <= max &&
                            Integer.parseInt(inputValues[1]) <= max) {
                        context.write(new Text(inputValues[0]), new Text(inputValues[1]));

                    }
                }

            }
        }
    }

    public static class FilterReducer extends Reducer<Text, Text, Text, Text> {

        private ArrayList<Integer> listA = new ArrayList<>();
        private ArrayList<Integer> listB = new ArrayList<>();


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            for (Text t : values) {
                context.write(null, new Text(key + "," + t));
            }
        }

    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("MAX", "1000");
        final Job job = Job.getInstance(conf, "MaxFilter");
        job.setJarByClass(MaxFilter.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean job1Success = job.waitForCompletion(true);
        if (job1Success) {
            return 1;
        } else {
            return 0;
        }
    }

    private static final Logger logger = LogManager.getLogger(MaxFilter.class);

    public static void main(final String[] args) {


        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new MaxFilter(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
