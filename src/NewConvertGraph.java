/**
 * Created by abiaps on 5/3/2017.
 */
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by abiaps on 4/23/2017.
 */
public class NewConvertGraph {

    public static void main(String[] args) throws FileNotFoundException {
        int count=0;
        ArrayList<String> uniqueNode=new ArrayList<String>();
        HashMap<Integer,Integer> h=new HashMap<Integer, Integer>();
        //File outfile=new File("outputFile.txt");
        File file = new File("/home/abiaps/Desktop/matrix/Google/web-Google.txt");
        Scanner in = new Scanner(file);
        int skipCount = 0;
        String line = "";
        while(in.hasNext()){ //while there are lines left to read in the file
            line = in.nextLine();
            skipCount++;
            if(skipCount==4) //skip over # comments in text file
                break;
        }
        while(in.hasNextLine()){
            line = in.nextLine(); //get the next line, numbers start here
            String lineItems="";
            String[] tokens = line.split("\\s+");
            int src=count;
            int dest=0;
            if(!h.containsKey(Integer.parseInt(tokens[0]))){
                h.put(Integer.parseInt(tokens[0]),count);
                src=count;
                count++;
            }
            else
                src=h.get(Integer.parseInt(tokens[0]));
            lineItems+="M,"+src;
            if(!h.containsKey(Integer.parseInt(tokens[1]))){
                h.put(Integer.parseInt(tokens[1]),count);
                dest=count;
                count++;
            }
            else
                dest=h.get(Integer.parseInt(tokens[1]));
            lineItems+=","+dest+",1";
            System.out.println(lineItems);
        }
        in.close();
        for(int i=0;i<count;i++)
            System.out.println("V,"+i+",0.00000114");
    }
}
