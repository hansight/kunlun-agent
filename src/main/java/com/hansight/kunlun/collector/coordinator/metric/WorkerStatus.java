package com.hansight.kunlun.collector.coordinator.metric;

import java.io.Serializable;

public class WorkerStatus implements Serializable{
	private static final long serialVersionUID = -3716294364909044340L;

	public enum CommonStatus{
		PENDING, // default 
		RUNNING,
		STOPED,
		UNKNOWN
	}
	
	public enum ConfigStatus{
		PENDING,
		SUCCESS,
		FAIL,
		UNKNOWN
	}
	
	private ProcessorType processorType = ProcessorType.UNKNOWN;
	
	private CommonStatus commonStatus = CommonStatus.UNKNOWN;
	
	public CommonStatus getCommonStatus() {
		return commonStatus;
	}

	public void setCommonStatus(CommonStatus jvmStatus) {
		this.commonStatus = jvmStatus;
	}

	private String processorId = "UNKNOWN"; // agent/forwarder id
	
	private long startTimestamp = -1L;
	
	private long lastestAccessTimestamp = -1L;
	
	private long currentCounter = -1L;
	
	
	public long getDuratioin(){
		return System.currentTimeMillis() - this.startTimestamp;
	}
	
	public long getHeartbeat(){
		return System.currentTimeMillis() - this.lastestAccessTimestamp;
	}
	
	public long getAverageEPS(){
		return this.getDuratioin()/ this.currentCounter;
	}

	public long getStartTimestamp() {
		return startTimestamp;
	}

	public void setStartTimestamp(long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}

	public long getLastestAccessTimestamp() {
		return lastestAccessTimestamp;
	}

	public void setLastestAccessTimestamp(long lastestAccessTimestamp) {
		this.lastestAccessTimestamp = lastestAccessTimestamp;
	}

	public long getCurrentCounter() {
		return currentCounter;
	}

	public void setCurrentCounter(long currentCounter) {
		this.currentCounter = currentCounter;
	}

	public ProcessorType getProcessorType() {
		return processorType;
	}

	public void setProcessorType(ProcessorType processorType) {
		this.processorType = processorType;
	}

	public String getProcessorId() {
		return processorId;
	}

	public void setProcessorId(String processorId) {
		this.processorId = processorId;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ (int) (currentCounter ^ (currentCounter >>> 32));
		result = prime * result
				+ ((commonStatus == null) ? 0 : commonStatus.hashCode());
		result = prime
				* result
				+ (int) (lastestAccessTimestamp ^ (lastestAccessTimestamp >>> 32));
		result = prime * result
				+ ((processorId == null) ? 0 : processorId.hashCode());
		result = prime * result
				+ ((processorType == null) ? 0 : processorType.hashCode());
		result = prime * result
				+ (int) (startTimestamp ^ (startTimestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WorkerStatus other = (WorkerStatus) obj;
		if (currentCounter != other.currentCounter)
			return false;
		if (commonStatus != other.commonStatus)
			return false;
		if (lastestAccessTimestamp != other.lastestAccessTimestamp)
			return false;
		if (processorId == null) {
			if (other.processorId != null)
				return false;
		} else if (!processorId.equals(other.processorId))
			return false;
		if (processorType != other.processorType)
			return false;
		if (startTimestamp != other.startTimestamp)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "WorkerStatus [processorType=" + processorType + ", jvmStatus="
				+ commonStatus + ", processorId=" + processorId
				+ ", startTimestamp=" + startTimestamp
				+ ", lastestAccessTimestamp=" + lastestAccessTimestamp
				+ ", currentCounter=" + currentCounter + "]";
	}
}