import org.apache.hadoop.io.Writable;
import java.io.DataInput; 
import java.io.DataOutput; 
import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;


public class myPair implements Writable{
	
	private double v1;
	private double v2;
	private double v3;
	private double d0 = 0.0;

	//default constructor
	public myPair(){
		this.v1 = d0;
		this.v2 = d0;
		this.v3 = d0;
	}

	//parameterized constructor
	public myPair(double v1, double v2, double v3){
		this.v1 = v1;
		this.v2 = v2;
		this.v3 = v3;
	}

	//setter method to set the value of myPair object
	public void set(double v1, double v2, double v3){
		this.v1 = v1;
		this.v2 = v2;
		this.v3 = v3;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// for deserialization
		v1=in.readDouble();
		v2=in.readDouble();
		v3=in.readDouble();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// for serialization
		out.writeDouble(v1);
		out.writeDouble(v2);
		out.writeDouble(v3);
	}

	 // TODO:
	 public double[] get(){
	 	double[] values = {this.v1, this.v2, this.v3};
	 	return values;
	 }
}	