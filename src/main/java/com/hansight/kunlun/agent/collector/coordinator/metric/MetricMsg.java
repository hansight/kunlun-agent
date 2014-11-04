package com.hansight.kunlun.agent.collector.coordinator.metric;

import java.io.Serializable;

import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus.CommonStatus;


public class MetricMsg implements Serializable{

	private static final long serialVersionUID = -4886648466785430317L;

	private String dataSourceId = "UNKNOWN";
	
	private CommonStatus commonStatus = CommonStatus.UNKNOWN;

	public String getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(String dataSourceId) {
		this.dataSourceId = dataSourceId;
	}

	public CommonStatus getCommonStatus() {
		return commonStatus;
	}

	public void setCommonStatus(CommonStatus jvmStatus) {
		this.commonStatus = jvmStatus;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dataSourceId == null) ? 0 : dataSourceId.hashCode());
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
		MetricMsg other = (MetricMsg) obj;
		if (dataSourceId == null) {
			if (other.dataSourceId != null)
				return false;
		} else if (!dataSourceId.equals(other.dataSourceId))
			return false;
		return true;
	}
}
