package org.apache.airavata.helix.impl.task.parsing.util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;

public class FileHelper {

    public String createLocalCopyOfDir(URI uri, String workingDir) throws Exception {
        if (uri.getScheme().equals("scp")) {
            String pubKeyFile = WorkerProperties.getInstance().getProperty(DataCatConstants.PUBLIC_KEY_FILE, "");
            String privateKeyFile = WorkerProperties.getInstance().getProperty(DataCatConstants.PRIVATE_KEY_FILE, "");

            String loginUsername;
            if (uri.getUserInfo() != null) {
                loginUsername = uri.getUserInfo();
            } else {
                loginUsername = WorkerProperties.getInstance().getProperty(DataCatConstants.SSH_LOGIN_USERNAME, "");
            }

            String passPhrase = WorkerProperties.getInstance().getProperty(DataCatConstants.PASS_PHRASE, "");

            SCPFileDownloader scpFileDownloader = new SCPFileDownloader(uri.getHost(), pubKeyFile,
                    privateKeyFile, loginUsername, passPhrase, 22);
            if (!workingDir.endsWith(File.separator)) {
                workingDir += File.separator;
            }
            String destFilePath = workingDir + Paths.get(uri.getPath()).getFileName().toString();
            scpFileDownloader.downloadFile(uri.getPath(), destFilePath);
            File file = new File(destFilePath);
            if (!file.exists()) {
                throw new Exception("Dir download failed for " + uri.toString());
            }
            return destFilePath;
        } else if (uri.getScheme().equals("file")) {
            if (!workingDir.endsWith(File.separator)) {
                workingDir += File.separator;
            }
            String destFilePath = workingDir + Paths.get(uri.getPath()).getFileName().toString();
            FileUtils.copyFile(new File(uri.getPath()), new File(destFilePath));
            return destFilePath;
        } else {
            throw new Exception("Unsupported file protocol");
        }
    }
}
