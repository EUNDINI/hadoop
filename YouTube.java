import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20180955
{
	public static class Data {
		public String category;
		public Double rating;
		
		public Data(String category, Double rating) {
			this.category = category;
			this.rating = rating;
		}
		
		public String getString()
		{
			return category + " " + rating;
		}
	}

	public static class EmpComparator implements Comparator<Data> {
		public int compare(Data x, Data y) {
			if ( x.rating > y.rating ) return 1;
			if ( x.rating < y.rating ) return -1;
			return 0;
		}
	}
	
	public static void insertEmp(PriorityQueue q, String category, Double rating, int topK) {
		Data emp_head = (Data) q.peek();
		if ( q.size() < topK || emp_head.rating < rating )
		{
			Data emp = new Data(category, rating);
			q.add( emp );
			if ( q.size() > topK ) q.remove();
		}	
	}

	public static class TopKMapper extends Mapper<Object, Text, Text, DoubleWritable> {		
		private Text category = new Text();
		private DoubleWritable rating = new DoubleWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = new String[7];
			int index = 0;
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "|");
			
			while (tokenizer.hasMoreTokens()) {
			    data[index++] = tokenizer.nextToken();
                    	}
                    	
                    	category.set(data[3]);
                    	rating.set(Double.parseDouble(data[6]));
		 	
		 	context.write(category, rating);
		}	
		
	}
	
	public static class TopKReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
		private PriorityQueue<Data> queue;
		private Comparator<Data> comp = new EmpComparator();
		private int topK;
	
		private DoubleWritable total = new DoubleWritable();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
                {
                	double sum = 0;
                	int count = 0;
                	
                	for (DoubleWritable val : values) {
                       	sum += val.get();
                       	count++;
                       }
                       
                       // total.set(sum / count);
                       // context.write(key, total);
                       sum = sum / count;
                       insertEmp(queue, key.toString(), sum, topK);
                }
                
                protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Data>( topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while( queue.size() != 0 ) {
				Data emp = (Data) queue.remove();
				context.write( new Text(emp.getString()), NullWritable.get() );
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int topK = Integer.parseInt(otherArgs[2]);

		if (otherArgs.length != 3) {
			System.err.println("Usage: TopK <in> <out>"); System.exit(2);
		}
		
		conf.setInt("topK", topK);
		Job job = new Job(conf, "TopK");
		job.setJarByClass(YouTubeStudent20180955.class);
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


