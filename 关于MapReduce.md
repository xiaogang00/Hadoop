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

构建Job对象之后，需要指定输入和输出数据的路径。