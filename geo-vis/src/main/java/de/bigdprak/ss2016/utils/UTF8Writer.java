package de.bigdprak.ss2016.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

public class UTF8Writer {

	private File f;
	private Writer wr;
	private boolean initialized = false;
	
	public UTF8Writer(String output_path) {
		this.f = new File(output_path);
		this.initialized = false;
		this.wr = null;
		try {
			this.wr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.f), "UTF8"));
			this.initialized = true;
		} catch (UnsupportedEncodingException e) {
			//e.printStackTrace();
			System.err.println(e.getMessage());
			this.initialized = false;
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
			System.err.println(e.getMessage());
			this.initialized = false;
		}
	}
	
	public boolean clear() {
		return TextFileWriter.writeOver(this.f, "");
	}
	
	public boolean appendLine(String s) {
		boolean ret = false;
		if (this.initialized) {
			try {
				this.wr.append(s).append("\r\n");
				ret = true;
			} catch (IOException e) {
				//e.printStackTrace();
				System.err.println(e.getMessage());
				ret = false;
			}
		}
		return ret;
	}
	
	public void close() {
		try {
			this.wr.flush();
			this.wr.close();
		} catch (IOException e) {
			//e.printStackTrace();
			System.err.println(e.getMessage());
		}
		this.wr = null;
		this.initialized = false;
	}
	
	public static void main(String[] args){

		try {
			File fileDir = new File("/home/balthorius/progs/hadoop/input/test.txt");
			
			Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileDir), "UTF8"));
			
			out.append("Website UTF-8").append("\r\n");
			out.append("?? UTF-8").append("\r\n");
			out.append("??????? UTF-8").append("\r\n");
			
			out.flush();
			out.close();
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
