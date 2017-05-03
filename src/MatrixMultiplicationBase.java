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

public class MatrixMultiplicationBase {

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
                outputValue.set(indicesAndValue[0]+","+indicesAndValue[1]+","+indicesAndValue[3]);
                outputKey.set(indicesAndValue[2]);
                context.write(outputKey, outputValue);
            }
            else if(indicesAndValue[0].equals("V")){
                // (V,id,val);
                outputKey.set(indicesAndValue[1]);
                outputValue.set(indicesAndValue[0]+","+indicesAndValue[2]);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class Reducer1
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] value;
            Text outputKey = new Text();
            Text outputValue = new Text();
            ArrayList<String> saved_v=new ArrayList<String>();
            HashMap<String,String> saved_kv=new HashMap<String,String>();
            String vectorkey=key.toString();
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("M")) {
                    saved_kv.put(value[1],value[2]);
                } else {
                    saved_v.add(value[1]);
                    outputKey.set("V");
                    outputValue.set(key+","+"self"+","+saved_v.get(0));
                    context.write(outputKey,outputValue);
                }
            }
            if(saved_kv.size()>0)	{
                for(Map.Entry<String,String> m:saved_kv.entrySet()){
                    outputKey.set("V");
                    outputValue.set(m.getKey()+","+"others"+","+Double.parseDouble(m.getValue())*Double.parseDouble(saved_v.get(0)));
                    context.write(outputKey, outputValue);
                }
            }
            else{
                outputKey.set("V");
                outputValue.set(vectorkey+","+"others"+","+"0");
                context.write(outputKey, outputValue);
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
                outputValue.set(line[1]+","+line[2]);
                context.write(outputKey,outputValue);
            }
            else{
                Text outputValue=new Text();
                Text outputKey=new Text();
                outputKey.set(line[0]);
                outputValue.set(line[1]+","+line[2]);
                context.write(outputKey,outputValue);
            }
        }
    }

    public static class Reducer2
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] value;
            double others_v=0,self_v=0;
            Text outputValue=new Text();

            for (Text val : values) {
                value = val.toString().split(",");
                if(value[0].equals("others"))
                    others_v+=Double.parseDouble(value[1]);
                else
                    self_v=Double.parseDouble(value[1]);
            }
            if(others_v!=0)
                outputValue.set(Double.toString(others_v));
            else
                outputValue.set("0");
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {

        // TODO Auto-generated method stub
	   /* if (args.length != 3) {
            System.err.println("Usage: MatrixMultiply <in_dir>    <temp_out_dir> <final_output_dir>");
            System.exit(2);
        }*/

        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")

        Job job1= Job.getInstance(conf,"Job1");
        job1.setJarByClass(MatrixMultiplicationBase.class);
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
        job2.setJarByClass(MatrixMultiplicationBase.class);
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

