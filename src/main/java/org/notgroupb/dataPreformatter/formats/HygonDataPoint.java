package org.notgroupb.dataPreformatter.formats;

import java.util.Date;

public class HygonDataPoint {
	
	private String name;
	
	private double lon;
	
	private double lat;
	
	private float measurement;
	
	private float mnw;
	
	private float mhw;
	
	private float average;
	
	private float level1;
	
	private float level2;
	
	private float level3;
	
	private Date recordTime;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
			this.lat = lat;
	}

	public float getMeasurement() {
		return measurement;
	}

	public void setMeasurement(float measurement) {
		this.measurement = measurement;
	}

	public float getMnw() {
		return mnw;
	}

	public void setMnw(float mnw) {
		this.mnw = mnw;
	}

	public float getMhw() {
		return mhw;
	}

	public void setMhw(float mhw) {
		this.mhw = mhw;
	}

	public float getLevel1() {
		return level1;
	}

	public void setLevel1(float level1) {
		this.level1 = level1;
	}

	public float getLevel2() {
		return level2;
	}

	public void setLevel2(float level2) {
		this.level2 = level2;
	}

	public float getLevel3() {
		return level3;
	}

	public void setLevel3(float level3) {
		this.level3 = level3;
	}

	public float getAverage() {
		return average;
	}

	public void setAverage(float average) {
		this.average = average;
	}

	public Date getRecordTime() {
		return recordTime;
	}

	public void setRecordTime(Date recordingTime) {
		this.recordTime = recordingTime;
	}
}
