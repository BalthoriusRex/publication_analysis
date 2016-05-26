package de.bigdprak.ss2016;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class HdfsTest {
	
	private String user_name = "bigprak";
	private String url = "wdi06.informatik.uni-leipzig.de";
	private String port = "8020"; // 8020

    public static void main(String args[]) {
    	
    	final HdfsTest test = new HdfsTest();

        try {
            UserGroupInformation ugi
                = UserGroupInformation.createRemoteUser(test.user_name);

            ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {

                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://" + test.url + ":" + test.port + "/user/" + test.user_name);
                    conf.set("hadoop.job.ugi", test.user_name);

                    FileSystem fs = FileSystem.get(conf);

                    //fs.createNewFile(new Path("/user/" + test.user_name + "/test"));

                    FileStatus[] status = fs.listStatus(new Path("/user/" + test.user_name));
                    for(int i=0;i<status.length;i++){
                        System.out.println(status[i].getPath());
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}