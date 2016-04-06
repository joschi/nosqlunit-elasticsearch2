package com.github.joschi.nosqlunit.elasticsearch2;

import com.google.common.io.Files;
import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class EmbeddedElasticsearchLifecycleManager extends AbstractLifecycleManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedElasticsearchLifecycleManager.class);
    private static final String LOCALHOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9300;
    private static final String HOME_PATH_PROPERTY = "path.home";
    private static final String DATA_PATH_PROPERTY = "path.data";

    public static final File EMBEDDED_ELASTICSEARCH_HOME_PATH = Files.createTempDir();
    public static final File EMBEDDED_ELASTICSEARCH_DATA_PATH = new File(EMBEDDED_ELASTICSEARCH_HOME_PATH, "data");

    private File homePath = EMBEDDED_ELASTICSEARCH_HOME_PATH;
    private File dataPath = EMBEDDED_ELASTICSEARCH_DATA_PATH;
    private Settings.Builder settingsBuilder = Settings.builder().put("node.local", true);

    @Override
    public String getHost() {
        return LOCALHOST + dataPath;
    }

    @Override
    public int getPort() {
        return DEFAULT_PORT;
    }

    @Override
    public void doStart() throws Throwable {
        LOGGER.info("Starting Embedded Elasticsearch instance.");

        final Settings settings = settingsBuilder
                .put(HOME_PATH_PROPERTY, homePath)
                .put(DATA_PATH_PROPERTY, dataPath)
                .build();

        final Node node = elasticsearchNode(settings);
        node.start();
        EmbeddedElasticsearchInstancesFactory.getInstance().addEmbeddedInstance(node, dataPath.getAbsolutePath());

        LOGGER.info("Started Embedded Elasticsearch instance.");
    }

    private Node elasticsearchNode(Settings settings) {
        return new Node(settings);
    }

    @Override
    public void doStop() {
        LOGGER.info("Stopping Embedded Elasticsearch instance.");

        try(final Node node = EmbeddedElasticsearchInstancesFactory.getInstance().getEmbeddedByTargetPath(dataPath.getAbsolutePath())) {
            // NOP
        } catch (IOException e) {
            LOGGER.error("Couldn't properly stop Embedded Elasticsearch instance.", e);
        }

        EmbeddedElasticsearchInstancesFactory.getInstance().removeEmbeddedInstance(dataPath.getAbsolutePath());
        LOGGER.info("Stopped Embedded Elasticsearch instance.");

    }

    public void setSettings(Settings settings) {
        settingsBuilder.put(settings);
    }

    public void setClusterName(String clusterName) {
        settingsBuilder.put("cluster.name", clusterName);
    }

    public void setData(boolean data) {
        settingsBuilder.put("node.data", data);
    }

    public void setLocal(boolean local) {
        settingsBuilder.put("node.local", local);
    }

    public File getDataPath() {
        return dataPath;
    }

    public void setDataPath(File dataPath) {
        this.dataPath = dataPath;
    }

    public File getHomePath() {
        return homePath;
    }

    public void setHomePath(File homePath) {
        this.homePath = homePath;
    }
}
