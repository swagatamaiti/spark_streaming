package com.mutliOrder.ETL.helper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSHelper {
	private static HDFSHelper instance = null;
	private static Configuration conf;

	public static HDFSHelper getInstance() throws IOException {
		if (instance == null) {
			synchronized (HDFSHelper.class) {
				if (instance == null) {
					instance = new HDFSHelper();
				}
			}
		}
		return instance;
	}

	private HDFSHelper() throws IOException {
		conf = new Configuration();
	}

	public FileStatus[] getStatus(String hdfsPath) {
		FileSystem fs;
		FileStatus[] status=null;
		Path path = new Path(hdfsPath);
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			if(fs.exists(path)){
				status = fs.listStatus(path);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return status;
	}
	
	public boolean ReadFile(String hdfsPath, String localPath) {
		FileSystem fs;
		Path path = new Path(hdfsPath);
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			if(fs.exists(path)){
				InputStream is = fs.open(path);
				IOUtils.copyBytes(is, new FileOutputStream(new File(localPath)), 2048, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public boolean getmerge(String hdfsPath, String localPath) {
		FileSystem fs;
		Path hdfsPathWay = new Path(hdfsPath);
		File localFile = new File(localPath);
		localFile.setWritable(true, false);
		localFile.delete();
		try {
			localFile.getParentFile().mkdirs();
			localFile.createNewFile();
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			if(fs.exists(hdfsPathWay)&&fs.isDirectory(hdfsPathWay)){
				OutputStream os = new FileOutputStream(localFile,true);
				FileStatus[] fileStatus = fs.listStatus(hdfsPathWay);
				for(FileStatus file:fileStatus){
					Path path = file.getPath();
					InputStream is = fs.open(path);
					byte[] buffer =new byte[2048];
	                int len=0;
	                while ((len=is.read(buffer))>0) {
	                    os.write(buffer, 0, len);
	                }
				}
				os.flush();
				os.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public boolean deleteFile(String hdfsPath){
		FileSystem fs;
		Path path = new Path(hdfsPath);
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf);
			if(fs.exists(path))
				fs.delete(path,true);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public boolean downloadFileFromHDFS(String src, String dst) {
		Path srcPath = new Path(src);
		Path dstPath = new Path(dst);
		FileSystem hdfs = null;
		try {
			hdfs = srcPath.getFileSystem(conf);
			if(hdfs.exists(dstPath))
				hdfs.copyToLocalFile(srcPath, dstPath);
			else
				return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	 public boolean uploadToHDFS(String src , String dst , Configuration conf){  
	        Path dstPath = new Path(dst);  
	        try{  
	            FileSystem hdfs = dstPath.getFileSystem(conf) ;  
	            hdfs.copyFromLocalFile(false, new Path(src), dstPath) ;  
	        }catch(IOException ie){  
	            ie.printStackTrace() ;  
	            return false ;  
	        }  
	        return true ;  
	    }  

}
