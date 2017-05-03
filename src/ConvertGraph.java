/**
 * Created by abiaps on 5/3/2017.
 */
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.io.File;
import java.util.HashMap;

public class ConvertGraph {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        HashMap<Integer, Integer> nodeHash = new HashMap<Integer, Integer>();
        String line = "";
        try{
            File file = new File("src/web-Google.txt"); //if file is known at compile time
            //File file = new File(args[0]); //if file is passed in at runtime
            Scanner in = new Scanner(file);
            //Reading the file
            int skipCount = 0;
            while(in.hasNext()){ //while there are lines left to read in the file
                line = in.nextLine();
                skipCount++;
                if(skipCount==4) //skip over # comments in text file
                    break;
            }
            while(in.hasNextLine()){ //while there are lines left to read in the file
                line = in.nextLine(); //get the next line, numbers start here
                //System.out.println("line: " + line);
				/* split the line based on a space delimiter
						 so if line = "1 2", tokens = {"1","2"} */
                String[] tokens = line.split("	");
                //System.out.println("i: " + tokens[0]);
                //System.out.println("j: " + tokens[1]);
                nodeHash.put(Integer.parseInt(tokens[0]), 1); //add number to hashmap
                nodeHash.put(Integer.parseInt(tokens[1]), 1); //add number to hashmap

            }
            in.close();
        }//End try
        catch(FileNotFoundException e)
        {
            System.err.println("File was either not found or it does not exist.");
            System.out.printf("\n");
        }//End catch
        //printing out the edges in the matrix (where there is a 1 stored in the array element)

        System.out.println("Number of unique nodes in dataset: " + nodeHash.size());

    }

}
