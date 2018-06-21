package org.notgroupb.dataPreformatter.formats;

import java.util.Date;

public final class PegelOnlineDataPoint {
	
	private String name;
	
	private double lon;
	
	private double lat;
	
	private Date recordTime;
	
	private double mhw;
	
	private double hhw;
	
	private double mw;
	
	private double mnw;
	
	private double cwl;
	
	private float measurement;
	
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

	public Date getRecordTime() {
		return recordTime;
	}

	public void setRecordTime(Date measurementTime) {
		this.recordTime = measurementTime;
	}

	public double getCwl() {
		return cwl;
	}

	public void setCwl(double cwl) {
		this.cwl = cwl;
	}

	public double getMnw() {
		return mnw;
	}

	public void setMnw(double mnw) {
		this.mnw = mnw;
	}

	public double getMw() {
		return mw;
	}

	public void setMw(double mw) {
		this.mw = mw;
	}

	public double getHhw() {
		return hhw;
	}

	public void setHhw(double hhw) {
		this.hhw = hhw;
	}

	public double getMhw() {
		return mhw;
	}

	public void setMhw(double mhw) {
		this.mhw = mhw;
	}

	public float getMeasurement() {
		return measurement;
	}

	public void setMeasurement(float measurement) {
		this.measurement = measurement;
	}
	
}