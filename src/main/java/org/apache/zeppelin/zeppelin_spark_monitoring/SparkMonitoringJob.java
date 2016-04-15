package org.apache.zeppelin.zeppelin_spark_monitoring;

public class SparkMonitoringJob {
	private String id;
	private String name;
	private String description;
	private String submissionTime;
	private String completionTime;
	private Double duration;
	private String status;
	private int numTasks;
	private int numCompletedTasks;
	private int numFailedTasks;
	private Long submisstionTimeMilis;
	private Long completionTimeMilis;
	
	public Long getSubmisstionTimeMilis() {
		return submisstionTimeMilis;
	}
	public void setSubmisstionTimeMilis(Long submisstionTimeMilis) {
		this.submisstionTimeMilis = submisstionTimeMilis;
	}
	public Long getCompletionTimeMilis() {
		return completionTimeMilis;
	}
	public void setCompletionTimeMilis(Long completionTimeMilis) {
		this.completionTimeMilis = completionTimeMilis;
	}
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
	public String getSubmissionTime() {
		return submissionTime;
	}
	public void setSubmissionTime(String submissionTime) {
		this.submissionTime = submissionTime;
	}
	public String getCompletionTime() {
		return completionTime;
	}
	public void setCompletionTime(String completionTime) {
		this.completionTime = completionTime;
	}
	public Double getDuration() {
		return duration;
	}
	public void setDuration(Double duration) {
		this.duration = duration;
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
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	
}
