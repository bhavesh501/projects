package hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class simplePartitioner extends Partitioner<TextPair, Text> {


@Override
public int getPartition(TextPair key, Text value, int numReduceTasks) {
	// TODO Auto-generated method stub
	
	System.out.println("The partitioner  partitioner  partitioner  partitioner  partitioner  *****************************************");
	String[] st= key.toString().split("###");
	
	//return st[1].hashCode();
	return 1;
}
}
