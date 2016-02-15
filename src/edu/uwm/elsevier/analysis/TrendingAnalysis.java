/**
 * 
 */
package edu.uwm.elsevier.analysis;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author qing
 *
 */
public class TrendingAnalysis {
	
	public void getTrendingScore(int[] years, int present, int window){
		Map<Integer, Integer> countMap = new HashMap<Integer, Integer>();
		for(int year: years){
			Integer count = countMap.get(year);
			if(count ==null){
				count=1;
			}else{
				count = count+1;
			}
			countMap.put(year, count);
		}
		
		Set<Integer> keys = countMap.keySet();
		Object[] keyObjs = keys.toArray();
		Arrays.sort(keyObjs, Collections.reverseOrder());
		int size = keyObjs.length;
		int i=0;
		int j=0;
		while(i<size ){
			int cur = (Integer)keyObjs[i];
			int prev = (Integer) keyObjs[i+1];
			Integer curCount = countMap.get(cur);
			Integer prevCount = countMap.get(prev);
			float increase = (curCount-prevCount)*1.0f/prevCount;
		}
	}
	
	

}
