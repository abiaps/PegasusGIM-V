/**
 * Created by abiaps on 5/3/2017.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiplyBlock {

    public static class Mapper1
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            // (M, idsrc, idst, Mij);
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (indicesAndValue[0].equals("M")) {
                outputValue.set(indicesAndValue[0]+","+indicesAndValue[1]+","+line.substring(6));
                outputKey.set(indicesAndValue[2]);
                context.write(outputKey, outputValue);
            }
            else if(indicesAndValue[0].equals("V")){
                // (V,id,val);
                outputKey.set(indicesAndValue[1]);
                outputValue.set(indicesAndValue[0]+","+line.substring(4));
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class Reducer1
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String value;
            Text outputKey = new Text();
            Text outputValue = new Text();
            HashMap<String,String> saved_v=new HashMap<String,String>();
            ArrayList<String> saved_kv=new ArrayList<String>();

            for (Text val : values) {
                value = val.toString();
                if(value.charAt(0)=='M')
                    saved_kv.add(value.substring(2));
                else if(value.charAt(0)=='V') {
                    StringTokenizer st=new StringTokenizer(value.substring(2),",");
                    while(st.hasMoreTokens()){
                        String cur=st.nextToken();
                        String next=st.nextToken();
                        saved_v.put(cur, next);
                    }

                    //    outputKey.set("Block_No");
                    //	outputValue.set(key+","+"self"+","+saved_v);
                    //	context.write(outputKey,outputValue);
                }
            }

            for(String matElement:saved_kv){
                String temp=""+matElement.charAt(0);
                StringTokenizer st1=new StringTokenizer(matElement.substring(2),",");
                while(st1.hasMoreTokens()){
                    outputKey.set("Block_No");
                    outputValue.set(temp+","+"others"+","+st1.nextToken()+","+Double.parseDouble(saved_v.get(st1.nextToken()))*Double.parseDouble(st1.nextToken().toString()));
                    context.write(outputKey, outputValue);
                }
                temp="";
            }

        }
    }

    public static class Mapper2
            extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] line = value.toString().split(",");
            if(line[1].equals("others")){
                Text outputValue=new Text();
                Text outputKey=new Text();
                outputKey.set(line[0]);
                outputValue.set(line[2]+","+line[3]);
                context.write(outputKey,outputValue);
            }
        }
    }

    public static class Reducer2
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            HashMap<String,Double> combineAll=new HashMap<String,Double>();
            Text outputValue=new Text();

            for (Text val : values) {
                String[] value = val.toString().split(",");
                if(!combineAll.containsKey(value[0]))
                    combineAll.put(value[0], Double.parseDouble(value[1]));
                else
                    combineAll.put(value[0],combineAll.get(value[0])+Double.parseDouble(value[1]));

            }
            outputValue.set(combineAll.toString());
            context.write(key
                    , outputValue);
        }
    }


    public static void main(String[] args) throws Exception {

        // TODO Auto-generated method stub
        if (args.length != 3) {
            System.err.println("Usage: MatrixMultiply <in_dir> <temp_out_dir> <final_output_dir>");
            System.exit(2);
        }


        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")

        Job job1= Job.getInstance(conf,"Job1");
        job1.setJarByClass(MatrixMultiplyBlock.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);


        //  Configuration conf2 = new Configuration();
        Job job2= Job.getInstance(conf,"Job2");
        job2.setJarByClass(MatrixMultiplyBlock.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);



    }

}
