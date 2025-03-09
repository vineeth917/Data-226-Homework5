package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class LongestWord {

    public static class LongestWordMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text("max");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String l = value.toString().replaceAll("[^a-zA-Z ]", " ").toLowerCase();
            StringTokenizer tokenizer = new StringTokenizer(l);
            int max = 0;
            List<String> longestwords = new ArrayList<>();

            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                int len = word.length();
                if (len > max) {
                    max = len;
                    longestwords.clear();
                    longestwords.add(word);
                } else if (len == maxLength) {
                    if (!longestwords.contains(word)) {
                        longestwords.add(word);
                    }
                }
            }
            // If we found any word, emitting the result with this format "length:word1,word2,..."
            if (max > 0) {
                StringBuilder sb = new StringBuilder();
                sb.append(max).append(":");
                for (int i = 0; i < longestwords.size(); i++) {
                    sb.append(longestwords.get(i));
                    if (i < longestwords.size() - 1) {
                        sb.append(",");
                    }
                }
                context.write(outKey, new Text(sb.toString()));
            }
        }
    }

    public static class LongestWordReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int globalmax = 0;
            List<String> result = new ArrayList<>();

            for (Text val : values) {
                // The value format is "length:word1,word2,..."
                String[] parts = val.toString().split(":", 2);
                int length = Integer.parseInt(parts[0]);
                String[] words = parts[1].split(",");

                if (length > globalmax) {
                    globalmax = length;
                    result.clear();
                    for (String word : words) {
                        result.add(word);
                    }
                } else if (length == globalmax) {
                    for (String word : words) {
                        if (!result.contains(word)) {
                            result.add(word);
                        }
                    }
                }
            }
            // Build a string for the output list of words.
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < result.size(); i++) {
                sb.append(result.get(i));
                if (i < result.size() - 1) {
                    sb.append(", ");
                }
            }
            context.write(new Text("Longest Word(s) with length " + globalmax), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LongestWord <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Longest Word Finder");
        job.setJarByClass(LongestWord.class);
        job.setMapperClass(LongestWordMapper.class);
        job.setReducerClass(LongestWordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
