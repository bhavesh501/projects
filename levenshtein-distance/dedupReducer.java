

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class dedupReducer extends Reducer<Text,Text,Text,Text>
{
	/*public public dedupReducer() {
		// TODO Auto-generated constructor stub
		super();
	}*/
	@Override
	 public void reduce(Text key, Iterable<Text> value,Context context) throws IOException, InterruptedException 
	 {
		 //super.reduce(key, value, context);
		// System.out.println("entered reduce phase *********&&&&&&$$$$$%%%%%###################");
		// Iterator<Text> itr= value.iterator();
		// Iterable<Text> itr2= itr.
		 ArrayList<String> ar= new ArrayList<String>();
			for(Text t1:value)
			{
				ar.add(t1.toString());
			}
			 try{
				 context.write(key, new Text(ar.toString()));
			 }
		 catch(Exception e)
		 {
			 e.printStackTrace();
		 }
		 
		 //ArrayList<String> duplicates=new ArrayList<String>();
		/*while(value.hasNext())
		 {
			 System.out.println("entered reduce phase,"+value.next().toString());
			 duplicates.add(value.next().toString());
			 duplicates.add(",,,,,,,,,,,,,,,,,\n");
		 }
		 context.write(key,new Text(duplicates.toString()));*/
	 }

	 @Override
	 public void run(Context context) throws IOException, InterruptedException {
			setup(context);
			while (context.nextKey()) {
				//dedupReducer dr= new dedupReducer();
				reduce(context.getCurrentKey(), context.getValues(), context);
			}
			cleanup(context);
		}

	

	
}