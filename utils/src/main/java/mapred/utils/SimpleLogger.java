package mapred.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Date;

public class SimpleLogger {
	public PrintStream out;
	public static final String INFO = "[INFO]";
	public static final String WARN = "[WARN]";
	public static final String EROR = "[EROR]";
	
	public SimpleLogger(Class<?> c){
		try {
			File f = new File("log");
			if(!f.isDirectory())
				f.mkdirs();
			out = new PrintStream(new FileOutputStream("log/"+c.getName()+".log", true));
		} catch(Exception e) {
			e.printStackTrace();
			out = System.out;
		}
	}
	
	public void info(Object s) {
		out.println(INFO + new Date().toString()+": "+s);
	}
	
	public void warn(Object s) {
		out.println(WARN + new Date().toString()+": "+s);
	}
	
	public void err(Object s) {
		out.println(EROR + new Date().toString()+": "+s);
	}
	
	public void close() {
		out.close();
	}
}