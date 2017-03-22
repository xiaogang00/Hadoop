import java.net.URL;

import sun.misc.IOUtils;

//3.5.1 从Hadoop  URL中读取数据
InputStream in = null;
try{
    in = new URL("hdfs://host/path").openStream();
    //precess in
}
finally {
    IOUtils.closeStream(in);
}

//范例3.1 通过URL StreamHeadler 实例以输出方式显示Hadoop文件系统的文件
public class URLCat{
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String[] args) throws Exception{
        InputStream in = null;
        try{
            in = new URL(args[0]).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally{
            IOUtils.closeStream(in);
        }
    }
}