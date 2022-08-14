import java.io.IOException;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;


public class UBERStudent20180955
{

        public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
                private Text word = new Text();
                private Text one_value = new Text();

                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                        String line = value.toString();
                        StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int i = 0;
			String s1 = "";
			String s2 = "";
                        while(tokenizer.hasMoreTokens()) {
                                if (i == 0) {
					s1 += tokenizer.nextToken();
				}
				else if (i == 1) {
					String[] week = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
					SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
					Calendar cal = Calendar.getInstance();
					Date getDate;
					
					try {
						getDate = format.parse(tokenizer.nextToken());
						cal.setTime(getDate);
						int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
						s1 += "," + week[w];
					} catch (Exception e) {
						e.printStackTrace();
					}
					//cal.setTime(getDate);
					//int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
					//s1 += "," + week[w];
				}
				else if (i == 2) {
					s2 += tokenizer.nextToken();
				}
				else {
					s2 += "," + tokenizer.nextToken();
				}
				i++;
                        }
			word.set(s1);
			one_value.set(s2);
			context.write(word, one_value);
                }
	}
	
	public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>
        {
                private Text result = new Text();
                public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
                {
			int num1 = 0;
			int num2 = 0;
                        for (Text val : values) {
				String[] strList = val.toString().split(",");
				num1 += Integer.parseInt(strList[0]);
				num2 += Integer.parseInt(strList[1]);
			}

			String total = Integer.toString(num2) + "," + Integer.toString(num1);
			result.set(total);
			context.write(key, result);

			for (Text val : values) {
				result.set(val);
				context.write(key, result);
			}
                }
        }

        public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length != 2)
                {
                        System.err.println("Usage: InvertedIndex <in> <out>");
                        System.exit(2);
                }


                Job job = new Job(conf, "inverted index");
                job.setJarByClass(UBERStudent20180955.class);
                job.setMapperClass(InvertedIndexMapper.class);
                job.setReducerClass(InvertedIndexReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setInputFormatClass(TextInputFormat.class);
                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

                FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
