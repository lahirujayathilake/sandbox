package org.apache.airavata.helix.impl.task.parsing.util;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class SSHUtils {

    private static final Logger logger = LoggerFactory.getLogger(SSHUtils.class);

    /**
     * This method will copy a remote file to a local directory
     *
     * @param remoteFile remote file path, this has to be a full qualified path
     * @param localFile  This is the local file to copy, this can be a directory too
     * @return returns the final local file path of the new file came from the remote resource
     */
    public static void scpFrom(String remoteFile, String localFile, Session session) {
        FileOutputStream fos = null;
        try {
            String prefix = null;
            if (new File(localFile).isDirectory()) {
                prefix = localFile + File.separator;
            }

            // exec 'scp -f remotefile' remotely
            String command = "scp -f " + remoteFile;
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);

            StandardOutReader stdOutReader = new StandardOutReader();
            ((ChannelExec) channel).setErrStream(stdOutReader.getStandardError());
            // get I/O streams for remote scp
            OutputStream out = channel.getOutputStream();
            InputStream in = channel.getInputStream();

            if (!channel.isClosed()) {
                channel.connect();
            }

            byte[] buf = new byte[1024];

            // send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();

            while (true) {
                int c = checkAck(in);
                if (c != 'C') {
                    break;
                }

                // read '0644 '
                in.read(buf, 0, 5);

                long fileSize = 0L;
                while (true) {
                    if (in.read(buf, 0, 1) < 0) {
                        // error
                        break;
                    }
                    if (buf[0] == ' ') {
                        break;
                    }
                    fileSize = fileSize * 10L + (long) (buf[0] - '0');
                }

                String file = null;
                for (int i = 0; ; i++) {
                    in.read(buf, i, 1);
                    if (buf[i] == (byte) 0x0a) {
                        file = new String(buf, 0, i);
                        break;
                    }
                }

                //System.out.println("fileSize="+fileSize+", file="+file);

                // send '\0'
                buf[0] = 0;
                out.write(buf, 0, 1);
                out.flush();

                // read a content of lfile
                fos = new FileOutputStream(prefix == null ? localFile : prefix + file);
                int foo;
                while (true) {
                    if (buf.length < fileSize) {
                        foo = buf.length;

                    } else {
                        foo = (int) fileSize;
                    }

                    foo = in.read(buf, 0, foo);
                    if (foo < 0) {
                        // error
                        break;
                    }
                    fos.write(buf, 0, foo);
                    fileSize -= foo;
                    if (fileSize == 0L) {
                        break;
                    }
                }
                fos.close();
                fos = null;

                if (checkAck(in) != 0) {
                    String error = "Error transfering the file content";
                    logger.error(error);
                    throw new Exception(error);
                }

                // send '\0'
                buf[0] = 0;
                out.write(buf, 0, 1);
                out.flush();
            }
            stdOutReader.onOutput(channel);
            if (stdOutReader.getStdErrorString().contains("scp:")) {
                throw new Exception(stdOutReader.getStdErrorString());
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);

        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (Exception ignored) {
            }
        }
    }

    private static int checkAck(InputStream in) throws IOException {
        int b = in.read();
        if (b == 0) return b;
        if (b == -1) return b;

        if (b == 1 || b == 2) {
            StringBuffer sb = new StringBuffer();
            int c;
            do {
                c = in.read();
                sb.append((char) c);
            }
            while (c != '\n');
            if (b == 1) { // error
                System.out.print(sb.toString());
            }
            if (b == 2) { // fatal error
                System.out.print(sb.toString());
            }
        }
        return b;
    }
}
