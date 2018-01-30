package hadoop;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class dedupDriver
{
	
public static void main(String[] args) throws IOException
{
	
	Configuration conf= new Configuration();
	Job job= new Job(conf,"lineCountDriver");
	job.setMapperClass(dedupMapper.class);
	job.setReducerClass(dedupReducer.class);
	job.setJarByClass(dedupDriver.class);
	job.setOutputKeyClass(TextPair.class);
	job.setOutputValueClass(Text.class);
	job.setPartitionerClass(simplePartitioner.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setGroupingComparatorClass(groupingComparator1.class);
	job.setSortComparatorClass(groupingComparator1.class);

	try {
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	} catch (ClassNotFoundException | InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
}



}