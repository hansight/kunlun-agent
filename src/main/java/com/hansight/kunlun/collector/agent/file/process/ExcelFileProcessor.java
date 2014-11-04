package com.hansight.kunlun.collector.agent.file.process;

import com.hansight.kunlun.collector.common.exception.LogWriteException;
import com.hansight.kunlun.collector.common.model.Event;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Author:zhhui
 * DateTime:2014/8/1 10:01.
 */
public abstract class ExcelFileProcessor extends FileProcessor {
    public abstract boolean getHasHeader();

    public abstract String getSeparator();

    @Override
    public void process() throws IOException, LogWriteException {
        reader = null;
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(new FileInputStream(path));
        SXSSFWorkbook workbook = new SXSSFWorkbook(xssfWorkbook);
        int numberOfSheets = workbook.getNumberOfSheets();
        long skip = position.getRecords();
        int rows = 0;
        int i = 0;
        for (int sheetNum = 0; sheetNum < numberOfSheets; sheetNum++) {
            Sheet sheet = workbook.getSheetAt(sheetNum);
            int lastRowNum = sheet.getLastRowNum();
            skip += getHasHeader() ? 1 : 0;

            rows += (lastRowNum + (getHasHeader() ? 0 : 1));
            if (lastRowNum < skip) {
                skip = skip - lastRowNum;
                continue;
            }
            int rowNum = (int) skip;
            for (; rowNum <= lastRowNum; rowNum++) {
                position.recordAdd();
                position.setPosition((long) lastRowNum);
                store.set(position);
                Row row = sheet.getRow(rowNum);
                int cellNums = row.getPhysicalNumberOfCells();
                Event event = new Event();
                Map<CharSequence, CharSequence> map = new LinkedHashMap<>();
                //  map.put("device", getApp());
                event.setHeader(map);
                StringBuilder builder = new StringBuilder();
                for (int cellNum = 0; cellNum < cellNums; cellNum++) {
                    builder.append(row.getCell(cellNum).getStringCellValue()).append(getSeparator());
                }
                event.setBody(ByteBuffer.wrap(builder.toString().getBytes()));
                write(event);
                if (!running) {
                    //停止 文件处理
                    break;
                }
            }
        }
        position.setRecords((long) rows);
    }
}
