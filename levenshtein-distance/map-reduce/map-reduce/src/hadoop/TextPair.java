package hadoop;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {

        
        Text first;

        
        Text second;

        public TextPair() {

        }

        public TextPair(Text t1) {
        	String str[]=t1.toString().split("###");
                first = new Text(str[0]);
                second = new Text(str[1]);
        }
        
       
        public void setFirstText(String t1) {
                first.set(t1.getBytes());
        }
        
       
        public void setSecondText(String t2) {
                second.set(t2.getBytes());
        }

       
        public Text getFirst() {
                return first;
        }

        
        public Text getSecond() {
                return second;
        }
        public String toString()
        {
        	return first.toString()+"###"+second.toString();
        }
        public void write(DataOutput out) throws IOException {
                first.write(out);
                second.write(out);
        }

        public void readFields(DataInput in) throws IOException {
                if (first == null)
                        first = new Text();

                if (second == null)
                        second = new Text();

                first.readFields(in);
                second.readFields(in);
        }

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
        public int hashCode() {
                return second.hashCode();
        }

        public boolean equals(Object o) {
                TextPair p = (TextPair) o;
                Double cmp = similarity(this.getSecond().toString(),p.getSecond().toString())*100;
                if(cmp>=70.00)
                	return true;
                else 
                	return false;
        }

		@Override
		public int compareTo(TextPair o) {
			TextPair ip2 = (TextPair) o;
            Double cmp = similarity(this.getSecond().toString(),ip2.getSecond().toString())*100;
            if(cmp>=70.00)
            	return 0;
            else 
            	return 1;
			
		}
}