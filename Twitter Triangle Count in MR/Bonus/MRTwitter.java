package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MRTwitter extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(MRTwitter.class);

    /**
     * Mapper class which emits 1 each time it encounters a follower for a user.
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text user = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] inputValues = itr.nextToken().split(",");
                if (inputValues.length == 2) {
                    user.set(inputValues[1]);
                    context.write(user, one);
                }
            }
        }
    }

    /**
     * Custom comparator for sorting keys by descending order.
     */
    public static class FollowersComparator extends WritableComparator {
        public  FollowersComparator() {
            super(Text.class,true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text key1 = (Text) w1;
            Text key2 = (Text) w2;
            return -1 * key1.compareTo(key2);
        }
    }

    /**
     * Followers class is the reducer
     */
    public static class FollowersReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable followers = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            followers.set(sum);
            context.write(key, followers);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "MRTwitter");
        job.setJarByClass(MRTwitter.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(FollowersReducer.class);
        job.setReducerClass(FollowersReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean job1Success = job.waitForCompletion(true);
        if (job1Success) {
            //Run the second map reduce job
            final Job job2 = Job.getInstance(conf, "Sort Job");
            job2.setJarByClass(MRTwitter.class);
            final Configuration job2Conf = job2.getConfiguration();
            job2Conf.set("mapreduce.output.textoutputformat.separator", " ");

            job2.setMapperClass(SortMapper.class);
            job2.setSortComparatorClass(FollowersComparator.class);
            job2.setReducerClass(SortReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job2, new Path("output"));
            FileOutputFormat.setOutputPath(job2, new Path("output2"));
            boolean job2Success = job2.waitForCompletion(true);
            return 1;
        } else {
            return 0;
        }
    }

    public static class SortMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable user;
        private Text followers = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] inputValues = itr.nextToken().split(",");
                user = new IntWritable(Integer.parseInt(inputValues[0]));
                followers.set(inputValues[1]);
                context.write(followers, user);

            }
        }
    }

    public static class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {

            for (final IntWritable val : values) {
                context.write(new Text(val.toString()), new IntWritable(Integer.parseInt(key.toString())));
            }


        }
    }


    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new MRTwitter(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
