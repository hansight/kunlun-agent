package com.hansight.kunlun.agent.collector.agent.tcp;

import com.hansight.kunlun.agent.collector.agent.utils.SyslogUtils;
import com.hansight.kunlun.agent.collector.common.exception.LogReaderException;
import com.hansight.kunlun.agent.collector.coordinator.metric.MetricException;
import com.hansight.kunlun.agent.collector.coordinator.metric.WorkerStatus.ConfigStatus;

/**
 * @author guanyi_ning
 */
public class SyslogTCPLogReader extends TCPLogReader {
	private SyslogUtils syslog;

	@Override
	public void setup() throws LogReaderException {
		syslog = new SyslogUtils();
	}

	@Override
	public void run() throws LogReaderException {
		try {
			syslog.launch("tcp", conf, metricService);
			metricService.setProcessorStatus(ConfigStatus.SUCCESS);
		} catch (Exception e) {
			try {
				metricService.setProcessorStatus(ConfigStatus.FAIL);
			} catch (MetricException e1) {
				e1.printStackTrace();
			}
			throw new LogReaderException("launch syslog tcp error", e);
		}
	}

	@Override
	public void cleanup() throws LogReaderException {
	}
}
