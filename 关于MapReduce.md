map函数是一个数据准备阶段，输入的是NCDC原始数据，比如可以选择文本数据作为输入格式。

map函数的输出经过MapReduce的框架处理智慧，最后发送到reduce函数。

#### Java MapReduce

```java
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
  extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWtitable>{
    private static final int MISSING = 9999;
    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException{
        String line = value.toString();
        String year = line.substring(15,19);
        int airTemperature;
        if(line.charAt(87) == '+'){
          airTemperature = Integer.parseInt(line.substring(88,92));
        }
        else{
          airTemperature = Integer.parseInt(line.substring(87,92));
        }
        String quality = line.substring(92,93);
        if(airTemperature != MISSING && quality.matches("[01459]")){
          context.write(new Text(year), new IntWritable(airTemperature));
        }
      }
  }
```



其中Mapper类是一个泛型类型，它有四个参数，分别指定map函数的输入键，输入值，输出键，输出值的类型。

Reduce类也是按照相似的方法进行定义，而Job对象指定作业执行规范。我们可以用它来控制整个作业的运行。我们再Hadoop集群上运行这个作业的时候，我们要把代码打包成一个JAR文件。

构建Job对象之后，需要指定输入和输出数据的路径。可以调用FileInputFormat类的静态方法addInputPath来定义输入数据的路径。这个路径可以是单个的文件。也可以用多次调用addInputPath()来实现多路径的输入。

setOutputKeyClass和setOutputValueClass控制map和reduce函数的输出类型，一般情况下都应该是相同的。

Job中的waitForCompletion( ) 方法提交作业并且等待执行完成。该方法中的布尔参数是一个详细标识，所以作业会把进度写到控制台。这个方法返回一个布尔值，表示执行的成ture败false



新旧的API之间有明显的区别：

* 新的API倾向于使用虚类，而不是接口，因为更加容易拓展
* 新的API放在org.apache.hadoop.mapreduce包中，之前的版本API依旧放在org.apache.hadoop.mapred中
* 新的API充分使用上下文对象，使得用户代码能够与MapReduce系统之间进行通信
* 键值对记录在这两类API中都被推给mapper和reducer。但是除此之外，新的API通过重写run( )方法允许mapper和reducer控制执行流程。
* 新的API中作业控制由Job类实现，而非旧API中的JobClient类
* 新增的API实现了配置的统一