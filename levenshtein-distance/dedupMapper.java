

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class dedupMapper extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) {
		Text second = new Text("Dummy data");

		try {
			String str[] = value.toString().split("###");
			if (str.length > 1) {
				// first = new Text(str[0]);
				second = new Text(str[1]);
			}

			context.write(second, second);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}
}