
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;

public class WordCountNoMR{
	
	//This function extracts data from the dataset and counts the words and save it in a Map
	public static void WordCountFile(String inputFile, Map<String, Integer> words)throws Exception{
		Scanner file=new Scanner(new File(inputFile));
		try {
			while(file.hasNext()){
				String word=file.next();
				Integer wordcount=words.get(word);
				if(wordcount != null) {
					wordcount++;
			}else {
				wordcount =1;
			}
				words.put(word, wordcount);
			
			}
			file.close();
		}catch(Exception e) {
			System.out.println(e);
		}

	}
	
	//This method writes the Map data to an output file.
	public static void writeMap(Map words, FileWriter filewriter) throws IOException {
	    Iterator itr = words.entrySet().iterator();
	    while (itr.hasNext()) {
	        Map.Entry word = (Map.Entry)itr.next();
	        String s=word.toString();
	        filewriter.write("\n"+s);
	        itr.remove(); 
	    }
	}

	//main method
	public static void main(String[] args) {
		try {
			long startTime = System.nanoTime();
			
			Map<String,Integer> words=new HashMap<String,Integer>();
			String inputFile= "/home/abhi/Desktop/BigData/HW1_Vuduthala_Abhijith/Part-1/dataset.txt";

			WordCountFile(inputFile,words);
			
			FileWriter filewriter=new FileWriter("/home/abhi/Desktop/BigData/HW1_Vuduthala_Abhijith/Part-1/output.txt");
			
			writeMap(words,filewriter);

			long endTime = System.nanoTime();
			System.out.println("Time taken to execute the job: "+((endTime-startTime)/100000)+" milliseconds");
			
			//go the end of the output file to see the time taken to execute the job.
			filewriter.write("\n\nTime taken to execute the job: "+((endTime-startTime)/100000)+" milliseconds");
			filewriter.close();
			
		}catch(Exception e) {
			System.out.println(e);
		}
	}
}
