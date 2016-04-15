package org.apache.zeppelin.zeppelin_spark_monitoring;

/**
 * Class presents the spark applications
 * @author duclv
 *
 */
public class SparkMonitoringApplication {
	private String id;
	private String name;
	private String startTime;
	private String endTime;
	private Double duration;
	private Boolean completed;
	private String sparkUser;
	
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
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public Double getDuration() {
		return duration;
	}
	public void setDuration(Double duration) {
		this.duration = duration;
	}
	public Boolean getCompleted() {
		return completed;
	}
	public void setCompleted(Boolean completed) {
		this.completed = completed;
	}
	public String getSparkUser() {
		return sparkUser;
	}
	public void setSparkUser(String sparkUser) {
		this.sparkUser = sparkUser;
	}
	
}
