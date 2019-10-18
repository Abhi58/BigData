

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Combination {
	 public static <T extends Comparable<? super T>> List<List<T>>findSortedCombinations(Collection<T> elements, int n) {
		 List<List<T>> result = new ArrayList<List<T>>();
	 
		  // handle initial step for recursion
		  if (n == 0) {
		  result.add(new ArrayList<T>());
		  return result;
		  }
		  // handle recursion for n-1
		  List<List<T>> combinations = findSortedCombinations(elements, n - 1);
		  for (List<T> combination: combinations) {
		  for (T element: elements) {
			  if (combination.contains(element)) {
				   continue;
				 }
				  
				  List<T> list = new ArrayList<T>();
				  list.addAll(combination);
				  
				  if (list.contains(element)) {
					  continue;
				  }
				  
				   list.add(element);
				   // sort items to avoid duplicate items
				   // example: (a, b, c) and (a, c, b) might be counted as
				   // different items if not sorted
				   Collections.sort(list);
				  
				   if (result.contains(list)) {
				   continue;
				   }
				   result.add(list);
				  }
				 }
				return result;
			}

}
