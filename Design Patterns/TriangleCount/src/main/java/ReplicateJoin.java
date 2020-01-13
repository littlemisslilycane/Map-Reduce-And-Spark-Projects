//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.DistributedFileSystem;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.TextOutputFormat;
//import org.apache.hadoop.mapreduce.Counters;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import javax.sound.sampled.Line;
//import java.io.*;
//import java.net.URI;
//import java.util.*;
//import java.util.stream.StreamSupport;
//import java.util.zip.DeflaterInputStream;
//import java.util.zip.GZIPInputStream;
//
//import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.getConf;
//
//public class ReplicateJoin {
//    public enum TRIANGLE_COUNTER {
//        COUNTER_VAL
//    }
//
//    public static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, IntWritable> {
//        private Map<String, LinkedList<String>> twitterEdges = new HashMap<>();
//
//        private static int counter;
//
//        @Override
//        public void setup(Context context) throws IOException, InterruptedException {
//            try {
//                Path[] files = DistributedCache.getLocalCacheFiles(context
//                        .getConfiguration());
//                if (files == null || files.length == 0) {
//                    throw new RuntimeException("No files in cache to distribute");
//                }
//                for (Path u : files) {
//                    BufferedReader rdr = new BufferedReader(
//                            new FileReader(
//                                    new File(u.getName() + "/edges")));
//
//                    String line;
//                    while ((line = rdr.readLine()) != null) {
//                        String[] edgeValues = line.split(",");
//                        if (edgeValues.length == 2) {
//                            if (twitterEdges.containsKey(edgeValues[0])) {
//                                twitterEdges.get(edgeValues[0]).add(edgeValues[1]);
//                            } else {
//                                LinkedList<String> follwingList = new LinkedList<>();
//                                follwingList.add(edgeValues[1]);
//                                twitterEdges.put(edgeValues[0], follwingList);
//
//                            }
//
//                        }
//                    }
//                }
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//
//        }
//
//        @Override
//        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
//            final StringTokenizer itr = new StringTokenizer(value.toString());
//
//            while (itr.hasMoreTokens()) {
//                String[] inputValues = itr.nextToken().split(",");
//                if (inputValues.length == 2) {
//                    MyWritable valueObject = new MyWritable(new Integer(inputValues[0]), new Integer(inputValues[1]), "XtoY");
//                    List<String> following = twitterEdges.get("" + valueObject.getY());
//                    if (following != null) {
//                        for (String i : following) {
//                            LinkedList<String> list = twitterEdges.get(i);
//                            if (list != null && list.contains(inputValues[0])) {
//                                ++counter;
//
//                            }
//
//                        }
//                    }
//                }
//            }
//        }
//
//        @Override
//        public void cleanup(Mapper.Context output) {
//
//            output.getCounter(TRIANGLE_COUNTER.COUNTER_VAL).increment(counter);
//            counter = 0;
//        }
//    }
//
//
//    public static void main(String[] args) throws Exception {
//        final Configuration conf = getConf();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (args.length != 2) {
//            System.err
//                    .println("Enter input and output path");
//            System.exit(1);
//        }
//
//        // Configure the join type
//        Job job = Job.getInstance(conf, "Replicated Join");
//
//        job.setJarByClass(ReplicateJoin.class);
//
//        job.setMapperClass(ReplicatedJoinMapper.class);
//
//        job.setNumReduceTasks(0);
//
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        // Configure the DistributedCache
//        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
//        DistributedCache.setLocalFiles(job.getConfiguration(), otherArgs[0]);
//
//        if(job.waitForCompletion(true) ){
//            Counters cn = job.getCounters();
//            System.out.println("Displaying the triangle count " + cn.findCounter(ReplicateJoin.TRIANGLE_COUNTER.COUNTER_VAL).getValue() / 3);
//
//        }
//
//    }
//
//}
