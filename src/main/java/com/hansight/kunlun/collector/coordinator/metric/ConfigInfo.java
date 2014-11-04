package com.hansight.kunlun.collector.coordinator.metric;

import java.io.Serializable;

import com.hansight.kunlun.collector.coordinator.metric.WorkerStatus.ConfigStatus;

/**
 * save datasource_Id,agent_id or forward_id,ProcessorType,ConfigStatus
 * @author Xr
 *
 */
public class ConfigInfo implements Serializable{
	private static final long serialVersionUID = -2563101746552290829L;
	private String datasourceId;
	private String processorId;	//agent_id or forward_id
	private ProcessorType processorType;
	private ConfigStatus configStatus;
	
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
	public ConfigStatus getConfigStatus() {
		return configStatus;
	}
	public void setConfigStatus(ConfigStatus configStatus) {
		this.configStatus = configStatus;
	}
	
	@Override
	public String toString() {
		return "ConfigInfo [datasourceId=" + datasourceId + ", processorId="
				+ processorId + ", processorType=" + processorType
				+ ", configStatus=" + configStatus + "]";
	}
}
