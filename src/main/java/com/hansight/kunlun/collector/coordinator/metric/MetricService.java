package com.hansight.kunlun.collector.coordinator.metric;

public class MetricService extends MetricBase{


	/**
	 * Default constructor <br />
	 * For Frontend to call
	 */
	public MetricService() {
		super();
	}

	/**
	 * For Backend to call
	 * @param processorId
	 * @param processorType
	 */
	public MetricService(String datasourceId, String processorId,ProcessorType processorType) {
		super();
		this.datasourceId = datasourceId;
		this.processorId = processorId;
		this.processorType = processorType;
	}
	
}
