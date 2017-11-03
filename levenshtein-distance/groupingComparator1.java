

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class groupingComparator1 extends WritableComparator {
	
	public groupingComparator1() {
        super(Text.class, true);
    }
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		Text t1= (Text) w1;
		Text t2= (Text) w2;
		String str1=t1.toString();
		String str2=t2.toString();
		//str1.compareTo(str2);
		System.out.println("The two strings which are getting compared are"+str1+" and "+str2);
		Double result=similarity(str1,str2)*100;
		if(result>=70.00)
		{
			System.out.println("The two strings matched are"+str1+" and "+str2);
			return 0;
			
		}
		else if(str1.length()>str2.length())
			return 1;
		else
			return -1;
		
	}
	/*@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
	{
		Text first1 = WritableComparator.(b1, s1);
	      int first2 = WritableComparator.readInt(b2, s2);
	}*/
	public double similarity(String s1, String s2) {
	    String longer = s1, shorter = s2;
	    if (s1.length() < s2.length()) { // longer should always have greater length
	      longer = s2; shorter = s1;
	    }
	    int longerLength = longer.length();
	    if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
	    /* // If you have StringUtils, you can use it to calculate the edit distance:
	    return (longerLength - StringUtils.getLevenshteinDistance(longer, shorter)) /
	                               (double) longerLength; */
	    return (longerLength - editDistance(longer, shorter)) / (double) longerLength;

	  }
	 public  int editDistance(String s1, String s2) {
		    s1 = s1.toLowerCase();
		    s2 = s2.toLowerCase();

		    int[] costs = new int[s2.length() + 1];
		    for (int i = 0; i <= s1.length(); i++) {
		      int lastValue = i;
		      for (int j = 0; j <= s2.length(); j++) {
		        if (i == 0)
		          costs[j] = j;
		        else {
		          if (j > 0) {
		            int newValue = costs[j - 1];
		            if (s1.charAt(i - 1) != s2.charAt(j - 1))
		              newValue = Math.min(Math.min(newValue, lastValue),
		                  costs[j]) + 1;
		            costs[j - 1] = lastValue;
		            lastValue = newValue;
		          }
		        }
		      }
		      if (i > 0)
		        costs[s2.length()] = lastValue;
		    }
		    return costs[s2.length()];
		  }
	/* public static void main(String[] args)
		{
			groupingComparator1 gc= new groupingComparator1();
			Text str1=new Text("10121600###Chuckanut Products 00002 10-Pound Premium Squirrel Diet Chuckanut products 00002 10-pound premium squirrel diet 10121600 0790004000020 Natures Nuts 790004000020 10 pounds 790004000020 B0006349EO");
			Text str2=new Text("10121600###Chuckanut Products 00002 10-Pound Premium Squirrel Diet Chuckanut products 00002 10-pound premium squirrel diet 10121600 0790004000020 Natures Nuts 790004000020 10 pounds 790004000020 B0006349EO");
			Object obj1= (Object) str1;
			Object obj2=(Object) str2;
			int a=gc.compare(obj1, obj2);
			System.out.println(a);
		}*/
	
}
