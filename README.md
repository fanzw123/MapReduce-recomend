# MapReduce-recomend
二度人脉好友推荐运用非常广泛，比如在一些主流的社交产品中就有可能认识的人这样的功能，一般来说可能认识的人是通过二度人脉搜索得到的，在传统的关系型数据库中，可以通过图的广度优先遍历算法实现，而且深度限定为2，然而在海量的数据中，这样的遍历成本太大，所以有必要利用MapReduce编程模型来并行化，本篇文章是二度好友推荐的mapreduce简单实现，难免会有不足和缺陷，希望大家能够指出，共同进步。

假如A和B是好友关系，B和C是好友关系，然而C和A不是好友关系，那么A和C是二度好友关系，他们可以通过B认识，B是中间人。我们定义一个符号“>”来代表follow，上面的例子可以这样表示
``` java
A>B
B>C
``` 
在社交网络任何一个活跃的用户U都存在对应的两个集合，一个是粉丝集合，一个是关注集合，以用户U作为中间联系的2度人脉对，是粉丝集合和关注集合的笛卡尔积。
于是在Map阶段，我们可以这样输出Map结果
``` java
key:“A ”value：“>B”;
key:“B ”value：“<A”;
key:“B ”value：“>C”;
key:“C ”value：“<B”;
``` 
在shuffle阶段，会自动合并相同key值的value
于是上面通过shuffle阶段，reduce的输入变成
``` java
"A":[">B"]
"B":["<A",">C"]
"C":["<B"]
``` 
所以在reduce阶段我们可以得到以Key为中间人的好友集合，其中粉丝集合的元素第一个字母是'<',关注集合的元素第一个字母是'>',分离这两个集合，并求他们的笛卡尔积，就可以得到二度人脉的关系对。
以下是Map和Reduce函数的核心代码：

测试输入:
``` 
liujia	qian
qian	sha
sha		yuan
``` 
测试输出:
``` 
liujia sha
qian yuan
``` 
Map核心代码
``` java
protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			
			String[] vaString=value.toString().split("\t");
			
			
			context.write(new Text(vaString[0]), new Text(">"+vaString[1]));
			context.write(new Text(vaString[1]), new Text("<"+vaString[0]));
		}
```


Reduce核心代码
``` java
protected void reduce(Text arg0, Iterable<Text> arg1,
				Reducer<Text, Text, Text, Text>.Context arg2)
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
		for(int i=0;i<buser.size();i++)
		{
			for(int j=0;j<fuser.size();j++)
			{
				arg2.write(new Text(buser.get(i)), new Text(fuser.get(j)));
				System.out.println(buser.get(i)+"->"+fuser.get(j));
			}
			
		}
			
			
			
		}
```
---
原创文章，转载请说明出处

