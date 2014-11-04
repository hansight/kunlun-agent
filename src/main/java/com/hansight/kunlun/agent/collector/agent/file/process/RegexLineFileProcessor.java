package com.hansight.kunlun.agent.collector.agent.file.process;

import com.hansight.kunlun.agent.collector.common.exception.LogWriteException;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class RegexLineFileProcessor extends LineFileProcessor {
    public abstract boolean getFinishedInEnd();

    @Override
    public void processWithLine() throws IOException, LogWriteException {
        Pattern pattern =getPattern();
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {

            position.recordAdd();
            position.setPosition((long) line.length() + lineSeparatorLength);
            if (!running) {
                break;
            }
            //  pos.posAdd();

            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                if (getFinishedInEnd()) {
                    builder.append(line);
                    save(builder.toString());
                    builder = new StringBuilder();
                } else {
                    save(builder.toString());
                    builder = new StringBuilder();
                    builder.append(line);
                }

            }
        }
        save(builder.toString());
    }

    public abstract Pattern getPattern();
}