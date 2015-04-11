package com.airdel.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AirlineEntry implements Writable {
	private String aname;
	private String fdate;
	private int airTimeDelay;
	private int distance;
	private int depDelay;
	private int arrDelay;
	private String dest;
	private String origin;
	private String fcode;
	private int num;
	// Overridden from Writable
	
	public void readFields(DataInput in) throws IOException {
		setAname(in.readLine());
		setFdate(in.readLine());
		setAirTimeDelay(in.readInt());
		setDistance(in.readInt());
		setDepDelay(in.readInt());
		setArrDelay(in.readInt());
		setDest(in.readLine());
		setOrigin(in.readLine());
		setFcode(in.readLine());
		setNum(in.readInt());
	}

	public void write(DataOutput out) throws IOException {
		out.writeChars(getAname());
		out.writeChars(getFdate());
		out.writeInt(getAirTimeDelay());
		out.writeInt(getDistance());
		out.writeInt(getDepDelay());
		out.writeInt(getArrDelay());
		out.writeChars(getDest());
		out.writeChars(getOrigin());
		out.writeInt(getNum());
	}

	// Overridden from Object
	/**
	 * @author nikit
	 */
	@Override
	public String toString() {
		return getAname();
	}
	
	@Override
	public AirlineEntry clone() {
		AirlineEntry clone = new AirlineEntry();
		clone.setAname(getAname());
		clone.setFdate(getFdate());
		clone.setAirTimeDelay(getAirTimeDelay());
		clone.setDistance(getDistance());
		clone.setDepDelay(getDepDelay());
		clone.setArrDelay(getArrDelay());
		clone.setDest(getDest());
		clone.setOrigin(getOrigin());
		clone.setNum(getNum());
		
		return clone;
	}

	// getters and setters
	
	public String getFdate() {
		return fdate;
	}

	public void setFdate(String fdate) {
		this.fdate = fdate;
	}

	public String getAname() {
		return aname;
	}

	public void setAname(String aname) {
		this.aname = aname;
	}

	public int getAirTimeDelay() {
		return airTimeDelay;
	}

	public void setAirTimeDelay(int airTimeDelay) {
		this.airTimeDelay = airTimeDelay;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public int getDepDelay() {
		return depDelay;
	}

	public void setDepDelay(int depDelay) {
		this.depDelay = depDelay;
	}

	public int getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(int arrDelay) {
		this.arrDelay = arrDelay;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getFcode() {
		return fcode;
	}

	public void setFcode(String fcode) {
		this.fcode = fcode;
	}
	
	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}
	
	public void incNum() {
		num++;
	}
}