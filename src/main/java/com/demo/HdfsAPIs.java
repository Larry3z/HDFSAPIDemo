package com.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;

public class HdfsAPIs {
    
    //创建新文件
    public static void createFile(String dst , byte[] contents) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
    }
    
    //上传本地文件
    public static void uploadFile(String src,String dst) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(src); //原路径
        Path dstPath = new Path(dst); //目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
            fs.copyFromLocalFile(false,srcPath, dstPath);

        
        //打印文件路径
        System.out.println("上传至 "+conf.get("fs.default.name"));
        System.out.println("------------文件列表------------"+"\n");
        FileStatus [] fileStatus = fs.listStatus(dstPath);
        for (FileStatus file : fileStatus) 
        {
            System.out.println(file.getPath());
        }
        fs.close();
    }
    
    //文件重命名
    public static void rename(String oldName,String newName) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isok = fs.rename(oldPath, newPath);
        if(isok){
            System.out.println("重命名成功");
        }else{
            System.out.println("重命名失败");
        }
        fs.close();
    }
    //删除文件
    public static void delete(String filePath) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        boolean isok = fs.deleteOnExit(path);
        if(isok){
            System.out.println("删除成功");
        }else{
            System.out.println("删除失败");
        }
        fs.close();
    }
    
    //创建目录
    public static void mkdir(String path) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(path);
        boolean isok = fs.mkdirs(srcPath);
        if(isok){
            System.out.println("创建目录成功");
        }else{
            System.out.println("创建目录失败");
        }
        fs.close();
    }
    
    //读取文件的内容
    public static void readFile(String filePath) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
        } finally {
            IOUtils.closeStream(in);
        }
    }

    // 查看HDFS文件的最后修改时间
    public static void getModifyTime(String filePath) throws IOException {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);
        Path dst = new Path(filePath);

        FileStatus files[] = hdfs.listStatus(dst);
        for (FileStatus file : files) {
            System.out.println(file.getPath() + "\t"
                    + file.getModificationTime());

            System.out.println(file.getPath() + "\t"
                    + new Date(file.getModificationTime()));

        }
        hdfs.close();
    }

    // 查看HDFS文件是否存在
    public static void exists(String filepath) throws IOException {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);
        Path dst = new Path(filepath);

        boolean ok = hdfs.exists(dst);
        System.out.println(ok ? "文件存在" : "文件不存在");
    }

    // 查看某个文件在HDFS集群的位置
    public static void fileBlockLocation(String filepath) throws IOException {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);
        Path dst = new Path(filepath);

        FileStatus fileStatus = hdfs.getFileStatus(dst);
        BlockLocation[] blockLocations = hdfs.getFileBlockLocations(fileStatus,
                0, fileStatus.getLen());
        for (BlockLocation block : blockLocations) {
            System.out.println(Arrays.toString(block.getHosts()) + "\t"
                    + Arrays.toString(block.getNames()));
        }
    }

    // 获取HDFS集群上所有节点名称
    public static void getHostName() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.150.135:9000");

        DistributedFileSystem hdfs = (DistributedFileSystem) FileSystem
                .get(conf);
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        for (DatanodeInfo dataNode : dataNodeStats) {
            System.out.println(dataNode.getHostName() + "\t"
                    + dataNode.getName());
        }
    }

    public static void main(String[] args) throws IOException {

        //测试创建文件
        /*byte[] contents =  "日立咨询\n".getBytes();
        createFile("input/test.txt",contents);*/

        //测试上传文件
        //uploadFile("output/part-r-00000", "input/");

        //测试重命名
        //rename("input/part-r-00000", "input/wangxintest");

        //测试新建目录
        //mkdir("input/wangxin");

        //测试删除文件
        //delete("input/part-r-00000"); //使用相对路径
        //delete("input/wangxin");    //删除目录

        //测试读取文件
        //readFile("input/1902.txt");

        //测试HDFS文件最后修改时间
        //getModifyTime("input/part-r-00000");

        //测试查看HDFS文件是否存在
        //exists("input/1901.txt");

        //测试查看某个文件在HDFS集群的位置
        //fileBlockLocation("input/1902.txt");

        //测试获取HDFS集群上所有节点名称
        getHostName();
    }

}