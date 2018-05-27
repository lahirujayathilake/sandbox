package org.apache.airavata.helix.impl.task.parsing.util;

import com.jcraft.jsch.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StandardOutReader {

    private static final Logger logger = LoggerFactory.getLogger(StandardOutReader.class);
    private String stdOutputString = null;
    private ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
    private int exitCode;

    public void onOutput(Channel channel) {
        try {
            StringBuffer pbsOutput = new StringBuffer("");
            InputStream inputStream = channel.getInputStream();
            byte[] tmp = new byte[1024];

            do {
                while (inputStream.available() > 0) {
                    int i = inputStream.read(tmp, 0, 1024);
                    if (i < 0) {
                        break;
                    }
                    pbsOutput.append(new String(tmp, 0, i));
                }
            } while (!channel.isClosed());

            String output = pbsOutput.toString();
            this.setStdOutputString(output);

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }


    public void exitCode(int code) {
        System.out.println("Program exit code - " + code);
        this.exitCode = code;
    }

    public int getExitCode() {
        return exitCode;
    }

    public String getStdOutputString() {
        return stdOutputString;
    }

    public void setStdOutputString(String stdOutputString) {
        this.stdOutputString = stdOutputString;
    }

    public String getStdErrorString() {
        return errorStream.toString();
    }

    public OutputStream getStandardError() {
        return errorStream;
    }
}
