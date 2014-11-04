package com.hansight.kunlun.collector.agent.udp;

import com.hansight.kunlun.collector.agent.utils.SyslogUtils;
import com.hansight.kunlun.collector.common.exception.LogReaderException;
import com.hansight.kunlun.collector.coordinator.metric.MetricException;
import com.hansight.kunlun.collector.coordinator.metric.WorkerStatus.ConfigStatus;

/**
 * 
 * @author guanyi_ning
 * 
 */
public class SyslogUDPLogReader extends UDPLogReader {
	private SyslogUtils syslog;

	@Override
	public void setup() throws LogReaderException {
		syslog = new SyslogUtils();
	}

	@Override
	public void run() throws LogReaderException {
		try {
			syslog.launch("udp", conf, metricService);
			metricService.setProcessorStatus(ConfigStatus.SUCCESS);
		} catch (Exception e) {
			try {
				metricService.setProcessorStatus(ConfigStatus.FAIL);
			} catch (MetricException e1) {
				e1.printStackTrace();
			}
			throw new LogReaderException("launch syslog udp error", e);
		}
	}

	@Override
	public void cleanup() throws LogReaderException {
	}

}
