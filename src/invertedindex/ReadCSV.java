package invertedindex;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ReadCSV {

	public static ArrayList<String> getStopWords(){
		ArrayList<String> stopwords= new ArrayList<String>();
		
		String csvFile = "stopwords.csv";
        String line = "";
        String cvsSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] words = line.split(cvsSplitBy);

              
                stopwords.add(words[0].toLowerCase());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
		//System.out.println(stopwords);
		return stopwords;
	}
	public static void main(String[] args){
		
	

		
	}
	
}
