package de.bigdprak.ss2016;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;


public class randomEdges {
	public static void main(String args[])
	{
		BufferedWriter wr = null;
		try {
			wr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("./Visualisierung/randomEdges.JSON")), "UTF8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
		//String string = "[";
		wr.write("var randomEdges = [");
		wr.flush();
			
		for(int i = 0; i <= 100000; i++)
		{	
			wr.write("["); //string += "[";
			for(int j = 0; j < 2; j++)
			{
				double lon = Math.random()*180. * ((Math.random() > 0.5) ? 1. : -1.);
				double lat  = Math.random()*90.  * ((Math.random() > 0.5) ? 1. : -1.);
	
				wr.write("["+lon+", "+lat+"]");//string += "["+lon+", "+lat+"]";
				if(j == 0)
				{
					wr.write(",\n");//string += ",";
				}
			}
			wr.write("]");//string += "]";
			if(i != 100000)
			{
				wr.write(",\n");//string += ",";
			}
		}
		wr.write("];");//string += "]";
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			wr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
