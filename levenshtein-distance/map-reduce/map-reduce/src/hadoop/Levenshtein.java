package hadoop;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
public class Levenshtein extends EvalFunc<Double>{
	
	
	  
	  
	    public Double exec(Tuple input1)  {
	        if (input1 == null || input1.size() == 0)
	            return null;
	        try{
	            String str = (String)input1.get(0);
	            String[] split=str.split("&&&&");
	            String[] s1=split[1].split("###");
	            String[] s2=split[3].split("###");
	            //split[1]= split[1].substring(0, split[1].length()-12);
	            /*String str1 = (String)input1.get(1);
	            String[] split1=str1.split("###");*/
	            
	            Double distance= similarity(s1[1].trim(),s2[1].trim())*100;
	           return distance;
	        }catch(Exception e){
	            e.printStackTrace();
	        }
	        return 0.00;
	        
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
	  

}
