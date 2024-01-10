package com.hyxiao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsOp {

    public static void main(String[] args) throws IOException {

        //  创建一个配置对象
        Configuration conf = new Configuration();
        //  指定HDFS的地址

        conf.set("fs.defaultFS", "hdfs://BigData:9000");
        //  获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);

        //  上传文件流
        put(fileSystem);
        //  下载文件流
//        download(fileSystem);

    }

    private static void download(FileSystem fileSystem) throws IOException {
        FSDataInputStream fis = fileSystem.open(new Path("/README.txt"));
        FileOutputStream fos = new FileOutputStream("E:\\test\\README.txt");
        IOUtils.copyBytes(fis, fos, 1024, true);
    }

    private static void put(FileSystem fileSystem) throws IOException {
        //  获取本地文件的输出流
        FileInputStream fis = new FileInputStream("/home/software/blog.txt");
        //  获取HDFS文件系统的输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/blog.txt"));
        //  上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis, fos, 1024, true);
    }

}
