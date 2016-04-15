package org.apache.zeppelin.zeppelin_spark_monitoring;

public class SparkMonitoringStage {
	private String id;
	private String name;
	private String status;
	private int numTasks;
	private int numCompletedTasks;
	private int numFailedTasks;
	private long inputBytes;
	private long inputRecords;
	private long outputBytes;
	private long outputRecords;
	private String details;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public int getNumTasks() {
		return numTasks;
	}
	public void setNumTasks(int numTasks) {
		this.numTasks = numTasks;
	}
	public int getNumCompletedTasks() {
		return numCompletedTasks;
	}
	public void setNumCompletedTasks(int numCompletedTasks) {
		this.numCompletedTasks = numCompletedTasks;
	}
	public int getNumFailedTasks() {
		return numFailedTasks;
	}
	public void setNumFailedTasks(int numFailedTasks) {
		this.numFailedTasks = numFailedTasks;
	}
	public long getInputBytes() {
		return inputBytes;
	}
	public void setInputBytes(long inputBytes) {
		this.inputBytes = inputBytes;
	}
	public long getInputRecords() {
		return inputRecords;
	}
	public void setInputRecords(long inputRecords) {
		this.inputRecords = inputRecords;
	}
	public long getOutputBytes() {
		return outputBytes;
	}
	public void setOutputBytes(long outputBytes) {
		this.outputBytes = outputBytes;
	}
	public long getOutputRecords() {
		return outputRecords;
	}
	public void setOutputRecords(long outputRecords) {
		this.outputRecords = outputRecords;
	}
	public String getDetails() {
		return details;
	}
	public void setDetails(String details) {
		this.details = details;
	}
	
}
