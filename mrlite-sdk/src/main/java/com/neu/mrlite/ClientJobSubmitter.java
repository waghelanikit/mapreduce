package com.neu.mrlite;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Locale;

public class ClientJobSubmitter {
	public static void main(String args[]) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException, MalformedURLException {
		try {
			if(args.length < 5)
				usage();
			Socket socket = new Socket(args[0], 2121);
		    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
		    BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
	    	out.println(args[1]+" "+args[2]+" "+args[3]+" "+args[4]);
			String line;
			while((line = in.readLine()) != null) {
				System.out.println(line);
			}
			socket.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void usage() {
		System.out.println("usage: java -jar maelite-sdk-*-SNAPSHOT.jar <MasterNodeIP> <JobJarLocation> <JobClass> <inFile> <outDir>");
		System.exit(1);
	}
}