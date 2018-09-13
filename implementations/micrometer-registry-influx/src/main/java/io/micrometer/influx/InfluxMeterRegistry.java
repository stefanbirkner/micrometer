/**
 * Copyright 2017 Pivotal Software, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micrometer.influx;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.*;
import io.micrometer.core.lang.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Jon Schneider
 */
public class InfluxMeterRegistry extends StepMeterRegistry {
    private final InfluxConfig config;
    private final Logger logger = LoggerFactory.getLogger(InfluxMeterRegistry.class);
    private boolean databaseExists = false;

    public InfluxMeterRegistry(InfluxConfig config, Clock clock, ThreadFactory threadFactory) {
        super(config, clock);
        this.config().namingConvention(new InfluxNamingConvention());
        this.config = config;
        start(threadFactory);
    }

    public InfluxMeterRegistry(InfluxConfig config, Clock clock) {
        this(config, clock, Executors.defaultThreadFactory());
    }

    private void createDatabaseIfNecessary() {
        if (!config.autoCreateDb() || databaseExists)
            return;

        HttpURLConnection con = null;
        try {
            String createDatabaseQuery = new CreateDatabaseQueryBuilder(config.db()).setRetentionDuration(config.retentionDuration())
                    .setRetentionPolicyName(config.retentionPolicy())
                    .setRetentionReplicationFactor(config.retentionReplicationFactor())
                    .setRetentionShardDuration(config.retentionShardDuration()).build();

            URL queryEndpoint = URI.create(config.uri() + "/query?q=" + URLEncoder.encode(createDatabaseQuery, "UTF-8")).toURL();

            con = createConnectionForPostRequest(queryEndpoint);

            int status = con.getResponseCode();

            if (status >= 200 && status < 300) {
                logger.debug("influx database {} is ready to receive metrics", config.db());
                databaseExists = true;
            } else if (status >= 400) {
                if (logger.isErrorEnabled()) {
                    logger.error("unable to create database '{}': {}", config.db(), IOUtils.toString(con.getErrorStream()));
                }
            }
        } catch (Throwable e) {
            logger.error("unable to create database '{}'", config.db(), e);
        } finally {
            quietlyCloseUrlConnection(con);
        }
    }

    @Override
    protected void publish() {
        createDatabaseIfNecessary();

        try {
            URL influxEndpoint = buildInfluxPublishUrl();
            for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
                publishBatch(batch, influxEndpoint);
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed InfluxDB publishing endpoint, see '" + config.prefix() + ".uri'", e);
        } catch (Throwable e) {
            logger.error("failed to send metrics", e);
        }
    }

    private URL buildInfluxPublishUrl() throws MalformedURLException {
        String write = "/write?consistency=" + config.consistency().toString().toLowerCase() + "&precision=ms&db=" + config.db();
        if (StringUtils.isNotBlank(config.retentionPolicy())) {
            write += "&rp=" + config.retentionPolicy();
        }
        return URI.create(config.uri() + write).toURL();
    }

    private void publishBatch(List<Meter> batch, URL influxEndpoint) throws IOException {
        HttpURLConnection con = null;
        try {
            con = createConnectionForPostRequest(influxEndpoint);
            con.setRequestProperty(HttpHeader.CONTENT_TYPE, MediaType.PLAIN_TEXT);
            con.setDoOutput(true);

            writeBatch(batch, con);

            int status = con.getResponseCode();

            if (status >= 200 && status < 300) {
                logger.info("successfully sent {} metrics to influx", batch.size());
                databaseExists = true;
            } else if (status >= 400) {
                if (logger.isErrorEnabled()) {
                    logger.error("failed to send metrics: {}", IOUtils.toString(con.getErrorStream()));
                }
            } else {
                logger.error("failed to send metrics: http {}", status);
            }

        } finally {
            quietlyCloseUrlConnection(con);
        }
    }

    private void authenticateRequest(HttpURLConnection con) {
        if (config.userName() != null && config.password() != null) {
            String encoded = Base64.getEncoder().encodeToString((config.userName() + ":" +
                    config.password()).getBytes(UTF_8));
            con.setRequestProperty(HttpHeader.AUTHORIZATION, "Basic " + encoded);
        }
    }

    private void writeBatch(List<Meter> batch, HttpURLConnection con) throws IOException {
        try (OutputStream os = getOutputStream(con)) {
            for (Meter meter: batch) {
                writeMeter(os, meter);
            }
            os.flush();
        }
    }

    private OutputStream getOutputStream(HttpURLConnection con) throws IOException {
        OutputStream os = con.getOutputStream();
        if (config.compressed()) {
            con.setRequestProperty(HttpHeader.CONTENT_ENCODING, HttpContentCoding.GZIP);
            return new GZIPOutputStream(os);
        } else {
            return os;
        }
    }

    private void writeMeter(OutputStream os, Meter meter) throws IOException {
        try {
            tryWriteMeter(os, meter);
        } catch (RuntimeException e) {
            logger.warn("Did not send Meter " + meter + " because it was not"
                + " possible to create the line protocol for it.", e);
        }
    }

    private void tryWriteMeter(OutputStream os, Meter m) throws IOException {
        if (m instanceof Timer) {
            writeTimer(os, (Timer) m);
        } else if (m instanceof DistributionSummary) {
            writeSummary(os, (DistributionSummary) m);
        } else if (m instanceof FunctionTimer) {
            writeTimer(os, (FunctionTimer) m);
        } else if (m instanceof TimeGauge) {
            writeGauge(os, m, ((TimeGauge) m).value(getBaseTimeUnit()));
        } else if (m instanceof Gauge) {
            writeGauge(os, m, ((Gauge) m).value());
        } else if (m instanceof FunctionCounter) {
            writeCounter(os, m, ((FunctionCounter) m).count());
        } else if (m instanceof Counter) {
            writeCounter(os, m, ((Counter) m).count());
        } else if (m instanceof LongTaskTimer) {
            writeLongTaskTimer(os, (LongTaskTimer) m);
        } else {
            writeAribtraryMeter(os, m);
        }
    }

    private void quietlyCloseUrlConnection(@Nullable HttpURLConnection con) {
        try {
            if (con != null) {
                con.disconnect();
            }
        } catch (Exception ignore) {
        }
    }

    class Field {
        final String key;
        final double value;

        Field(String key, double value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return key + "=" + DoubleFormat.decimalOrNan(value);
        }
    }

    private void writeAribtraryMeter(OutputStream os, Meter m) throws IOException {
        List<Field> fields = new ArrayList<>();

        for (Measurement measurement : m.measure()) {
            String fieldKey = measurement.getStatistic().toString()
                    .replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
            fields.add(new Field(fieldKey, measurement.getValue()));
        }

        writeLine(os, m, "unknown", fields.toArray(new Field[fields.size()]));
    }

    private void writeLongTaskTimer(OutputStream os, LongTaskTimer timer) throws IOException {
        writeLine(os, timer, "long_task_timer",
            new Field("active_tasks", timer.activeTasks()),
            new Field("duration", timer.duration(getBaseTimeUnit()))
        );
    }

    private void writeCounter(OutputStream os, Meter gauge, double count) throws IOException {
        writeLine(os, gauge, "counter", new Field("value", count));
    }

    private void writeGauge(OutputStream os, Meter gauge, Double value) throws IOException {
        if (!value.isNaN()) {
            writeLine(os, gauge, "gauge", new Field("value", value));
        }
    }

    private void writeTimer(OutputStream os, FunctionTimer timer) throws IOException {
        writeLine(os, timer, "histogram",
            new Field("sum", timer.totalTime(getBaseTimeUnit())),
            new Field("count", timer.count()),
            new Field("mean", timer.mean(getBaseTimeUnit()))
        );
    }

    private void writeTimer(OutputStream os, Timer timer) throws IOException {
        writeLine(os, timer, "histogram",
            new Field("sum", timer.totalTime(getBaseTimeUnit())),
            new Field("count", timer.count()),
            new Field("mean", timer.mean(getBaseTimeUnit())),
            new Field("upper", timer.max(getBaseTimeUnit()))
        );
    }

    private void writeSummary(OutputStream os, DistributionSummary summary) throws IOException {
        writeLine(os, summary, "histogram",
            new Field("sum", summary.totalAmount()),
            new Field("count", summary.count()),
            new Field("mean", summary.mean()),
            new Field("upper", summary.max())
        );
    }

    private void writeLine(OutputStream os, Meter meter, String metricType,
            Field... fields) throws IOException {
        writeName(os, meter);
        writeTags(os, meter);
        writeType(os, metricType);
        os.write(' ');
        writeFields(os, fields);
        os.write(' ');
        writeTimestamp(os);
        os.write('\n');
    }

    private void writeName(OutputStream os, Meter meter) throws IOException {
        write(os, getConventionName(meter.getId()));
    }

    private void writeTags(OutputStream os, Meter meter) throws IOException {
        for (Tag tag: getConventionTags(meter.getId())) {
            writeTag(os, tag.getKey(), tag.getValue());
        }
    }

    private void writeType(OutputStream os, String metricType) throws IOException {
        writeTag(os, "metric_type", metricType);
    }

    private void writeTag(OutputStream os, String key, String value) throws IOException {
        os.write(',');
        write(os, key);
        os.write('=');
        write(os, value);
    }

    private void writeFields(OutputStream os, Field... fields) throws IOException {
        boolean firstFieldWritten = false;
        for (Field field: fields) {
            if (firstFieldWritten) {
                os.write(',');
            } else {
                firstFieldWritten = true;
            }
            write(os, field.toString());
        }
    }

    private void writeTimestamp(OutputStream os) throws IOException {
        String timestamp = Long.toString(clock.wallTime());
        write(os, timestamp);
    }

    private void write(OutputStream os, String text) throws IOException {
        os.write(text.getBytes(UTF_8));
    }

    @Override
    protected final TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    private HttpURLConnection createConnectionForPostRequest(URL url) throws IOException {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setConnectTimeout((int) config.connectTimeout().toMillis());
        con.setReadTimeout((int) config.readTimeout().toMillis());
        con.setRequestMethod(HttpMethod.POST);
        authenticateRequest(con);
        return con;
    }
}
