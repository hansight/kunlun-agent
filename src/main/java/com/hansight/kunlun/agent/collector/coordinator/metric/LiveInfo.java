package com.hansight.kunlun.agent.collector.coordinator.metric;

import java.io.Serializable;

import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus.CommonStatus;



public class LiveInfo implements Serializable {
	private static final long serialVersionUID = -103952649675341441L;
	private String processorId;
	private ProcessorType processorType;
	private CommonStatus commonStatus;
	private String datasourceId;
	public String getDatasourceId() {
		return datasourceId;
	}
	public void setDatasourceId(String datasourceId) {
		this.datasourceId = datasourceId;
	}
	public String getProcessorId() {
		return processorId;
	}
	public void setProcessorId(String processorId) {
		this.processorId = processorId;
	}
	public ProcessorType getProcessorType() {
		return processorType;
	}
	public void setProcessorType(ProcessorType processorType) {
		this.processorType = processorType;
	}
	public CommonStatus getCommonStatus() {
		return commonStatus;
	}
	public void setCommonStatus(CommonStatus commonStatus) {
		this.commonStatus = commonStatus;
	}
	
	@Override
	public String toString() {
		return "LiveInfo [processorId=" + processorId + ", processorType="
				+ processorType + ", commonStatus=" + commonStatus
				+ ", datasourceId=" + datasourceId + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((processorId == null) ? 0 : processorId.hashCode());
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
		LiveInfo other = (LiveInfo) obj;
		if (processorId == null) {
			if (other.processorId != null)
				return false;
		} else if (!processorId.equals(other.processorId))
			return false;
		return true;
	}
}
