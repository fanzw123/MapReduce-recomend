package mian;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hamcrest.core.Is;



public class socity  {

	
	public static class map extends Mapper<LongWritable, Text, Text, Text> {

		
		
		
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub		
			String[] vaString=value.toString().split("\t");		
			context.write(new Text(vaString[0]), new Text(">"+vaString[1]));	
			context.write(new Text(vaString[1]), new Text("<"+vaString[0]));
		
		}
		
		
		
		
	}
	
	public static class re extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1,
				Context arg2)
				throws IOException, InterruptedException {

			List<String> fuser= new ArrayList<String>();
			List<String> buser= new ArrayList<String>();
			
			Iterator<Text> it=	arg1.iterator();
			
			while (it.hasNext()) {
				String item=it.next().toString();
				
				 if(item.charAt(0)=='>')
				 {
					 fuser.add(item.toString().substring(1));
				 }
				 else {
					 buser.add(item.toString().substring(1));
				}
			}
	

		//buser与fuser笛卡尔积
		for(int i=0;i<buser.size();i++)
		{
			for(int j=0;j<fuser.size();j++)
			{
				System.out.println(buser.get(i)+"->"+fuser.get(j));
				arg2.write(new Text(buser.get(i)), new Text(fuser.get(j)));
	
			}			
		}
	
		}		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();           

		Job job = new Job(conf,"socity");
		
		job.setJarByClass(socity.class); //设置运行jar中的class名称
		
		job.setMapperClass(map.class);//设置mapreduce中的mapper reducer combiner类
		job.setReducerClass(re.class);
      
		job.setOutputKeyClass(Text.class); //设置输出结果键值对类型
        job.setOutputValueClass(Text.class);
		String inputstring="hdfs://localhost:9000/input/socity.txt";
		String outstring="hdfs://localhost:9000/out";
		FileInputFormat.addInputPath(job,new Path(inputstring));//设置mapreduce输入输出文件路径
		FileOutputFormat.setOutputPath(job,new Path(outstring));	
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
