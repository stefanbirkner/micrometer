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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

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

            con = (HttpURLConnection) queryEndpoint.openConnection();
            con.setConnectTimeout((int) config.connectTimeout().toMillis());
            con.setReadTimeout((int) config.readTimeout().toMillis());
            con.setRequestMethod(HttpMethod.POST);
            authenticateRequest(con);

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
            con = (HttpURLConnection) influxEndpoint.openConnection();
            con.setConnectTimeout((int) config.connectTimeout().toMillis());
            con.setReadTimeout((int) config.readTimeout().toMillis());
            con.setRequestMethod(HttpMethod.POST);
            con.setRequestProperty(HttpHeader.CONTENT_TYPE, MediaType.PLAIN_TEXT);
            con.setDoOutput(true);

            authenticateRequest(con);

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
                    config.password()).getBytes(StandardCharsets.UTF_8));
            con.setRequestProperty(HttpHeader.AUTHORIZATION, "Basic " + encoded);
        }
    }

    private void writeBatch(List<Meter> batch, HttpURLConnection con) throws IOException {
        List<String> bodyLines = batch.stream()
            .flatMap(this::toLinesSafe)
            .collect(toList());

        String body = String.join("", bodyLines);

        try (OutputStream os = getOutputStream(con)) {
            os.write(body.getBytes());
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

    private Stream<String> toLinesSafe(Meter m) {
        try {
            return toLines(m);
        } catch (RuntimeException e) {
            logger.warn("Did not send Meter " + m + " because it was not"
                + " possible to create the line protocol for it.", e);
            return Stream.empty();
        }
    }

    private Stream<String> toLines(Meter m) {
        if (m instanceof Timer) {
            return writeTimer((Timer) m);
        }
        if (m instanceof DistributionSummary) {
            return writeSummary((DistributionSummary) m);
        }
        if (m instanceof FunctionTimer) {
            return writeTimer((FunctionTimer) m);
        }
        if (m instanceof TimeGauge) {
            return writeGauge(m.getId(), ((TimeGauge) m).value(getBaseTimeUnit()));
        }
        if (m instanceof Gauge) {
            return writeGauge(m.getId(), ((Gauge) m).value());
        }
        if (m instanceof FunctionCounter) {
            return writeCounter(m.getId(), ((FunctionCounter) m).count());
        }
        if (m instanceof Counter) {
            return writeCounter(m.getId(), ((Counter) m).count());
        }
        if (m instanceof LongTaskTimer) {
            return writeLongTaskTimer((LongTaskTimer) m);
        }
        return writeMeter(m);
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

    private Stream<String> writeMeter(Meter m) {
        Stream.Builder<Field> fields = Stream.builder();

        for (Measurement measurement : m.measure()) {
            String fieldKey = measurement.getStatistic().toString()
                    .replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
            fields.add(new Field(fieldKey, measurement.getValue()));
        }

        return Stream.of(influxLineProtocol(m.getId(), "unknown", fields.build()));
    }

    private Stream<String> writeLongTaskTimer(LongTaskTimer timer) {
        Stream<Field> fields = Stream.of(
                new Field("active_tasks", timer.activeTasks()),
                new Field("duration", timer.duration(getBaseTimeUnit()))
        );
        return Stream.of(influxLineProtocol(timer.getId(), "long_task_timer", fields));
    }

    private Stream<String> writeCounter(Meter.Id id, double count) {
        return Stream.of(influxLineProtocol(id, "counter", Stream.of(new Field("value", count))));
    }

    private Stream<String> writeGauge(Meter.Id id, Double value) {
        return value.isNaN() ? Stream.empty() :
                Stream.of(influxLineProtocol(id, "gauge", Stream.of(new Field("value", value))));
    }

    private Stream<String> writeTimer(FunctionTimer timer) {
        Stream<Field> fields = Stream.of(
                new Field("sum", timer.totalTime(getBaseTimeUnit())),
                new Field("count", timer.count()),
                new Field("mean", timer.mean(getBaseTimeUnit()))
        );

        return Stream.of(influxLineProtocol(timer.getId(), "histogram", fields));
    }

    private Stream<String> writeTimer(Timer timer) {
        final Stream<Field> fields = Stream.of(
                new Field("sum", timer.totalTime(getBaseTimeUnit())),
                new Field("count", timer.count()),
                new Field("mean", timer.mean(getBaseTimeUnit())),
                new Field("upper", timer.max(getBaseTimeUnit()))
        );

        return Stream.of(influxLineProtocol(timer.getId(), "histogram", fields));
    }

    private Stream<String> writeSummary(DistributionSummary summary) {
        final Stream<Field> fields = Stream.of(
                new Field("sum", summary.totalAmount()),
                new Field("count", summary.count()),
                new Field("mean", summary.mean()),
                new Field("upper", summary.max())
        );

        return Stream.of(influxLineProtocol(summary.getId(), "histogram", fields));
    }

    private String influxLineProtocol(Meter.Id id, String metricType, Stream<Field> fields) {
        String tags = getConventionTags(id).stream()
                .map(t -> "," + t.getKey() + "=" + t.getValue())
                .collect(joining(""));

        return getConventionName(id)
                + tags + ",metric_type=" + metricType + " "
                + fields.map(Field::toString).collect(joining(","))
                + " " + clock.wallTime()
                + "\n";
    }

    @Override
    protected final TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

}
