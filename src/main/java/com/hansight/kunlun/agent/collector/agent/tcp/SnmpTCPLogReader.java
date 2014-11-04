package com.hansight.kunlun.agent.collector.agent.tcp;

import com.hansight.kunlun.agent.collector.agent.utils.SnmpUtils;
import com.hansight.kunlun.agent.collector.common.exception.LogReaderException;
import com.hansight.kunlun.agent.collector.coordinator.metric.MetricException;
import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus.ConfigStatus;



import java.io.IOException;

public class SnmpTCPLogReader extends TCPLogReader {
	private SnmpUtils snmp;

	@Override
	public void setup() throws LogReaderException {
		snmp = new SnmpUtils();
	}

	@Override
	public void run() throws LogReaderException {
		try {
			snmp.launch("tcp", conf, metricService);
			metricService.setProcessorStatus(ConfigStatus.SUCCESS);
		} catch (IOException | MetricException e) {
			try {
				metricService.setProcessorStatus(ConfigStatus.FAIL);
			} catch (MetricException e1) {
				e1.printStackTrace();
			}
			throw new LogReaderException("launch snmp tcp error", e);
		}
	}

	@Override
	public void cleanup() throws LogReaderException {

	}

}
