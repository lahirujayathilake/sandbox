package org.apache.airavata.helix.impl.task.parsing.util;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

public class SCPFileDownloader {

    private Session session;

    public SCPFileDownloader(String hostName, String pubKeyFile, String privateKeyFile, String loginUsername, String passPhrase, Integer port)
            throws JSchException, IOException {
        Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");
        JSch jSch = new JSch();
        jSch.addIdentity(UUID.randomUUID().toString(), Files.readAllBytes(Paths.get(privateKeyFile)),
                Files.readAllBytes(Paths.get(pubKeyFile)), passPhrase.getBytes());
        session = jSch.getSession(loginUsername, hostName, port);
        session.setConfig(config);
        session.connect();
    }

    public void downloadFile(String remotePath, String localPath) throws JSchException {
        if (!session.isConnected()) {
            session.connect();
        }
        SSHUtils.scpFrom(remotePath, localPath, session);
    }
}
