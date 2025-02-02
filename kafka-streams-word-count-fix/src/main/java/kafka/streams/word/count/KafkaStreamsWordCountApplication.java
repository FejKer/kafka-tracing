/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.streams.word.count;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
public class KafkaStreamsWordCountApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsWordCountApplication.class, args);
	}

	public static class WordCountProcessorApplication {

		public static final String INPUT_TOPIC = "input";
		public static final String OUTPUT_TOPIC = "output";
		public static final int WINDOW_SIZE_MS = 30_000;

		//@Bean
//		public Function<KStream<Bytes, String>, KStream<Bytes, WordCount>> process() {
//
//			return input -> input
//					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
//					.map((key, value) -> new KeyValue<>(value, value))
//					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
//					.windowedBy(TimeWindows.of(Duration.ofMillis(WINDOW_SIZE_MS)))
//					.count(Materialized.as("WordCounts-1"))
//					.toStream()
//					.map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))));
//		}
	}

	private String applicationName = "kafka-streams-app";

	private String endpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");

	@Bean
	public OpenTelemetry openTelemetry() {
		Resource resource = Resource.getDefault()
				.merge(Resource.create(Attributes.of(
						ResourceAttributes.SERVICE_NAME, applicationName
				)));

		OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
				.setEndpoint(endpoint)
				.build();

		SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
				.addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
				.setResource(resource)
				.build();

		return OpenTelemetrySdk.builder()
				.setTracerProvider(sdkTracerProvider)
				.setPropagators(ContextPropagators.noop())
				.buildAndRegisterGlobal();
	}

	@Bean
	public Tracer tracer(OpenTelemetry openTelemetry) {
		return openTelemetry.getTracer("kafka-streams-processor");
	}

	@Configuration
	public class KafkaStreamConfig {

		private final Tracer tracer;
		private static final long WINDOW_SIZE_MS = 30000;

		public KafkaStreamConfig(Tracer tracer) {
			this.tracer = tracer;
		}

		@Bean
		public Function<KStream<Bytes, String>, KStream<Bytes, WordCount>> process() {
			return input -> input.transform(() -> new TransformerWithTracing<>(tracer))
					.flatMapValues(value -> {
						Span span = tracer.spanBuilder("flatMap-split-words")
								.setParent(Context.current())
								.setSpanKind(SpanKind.INTERNAL)
								.startSpan();

						try (Scope scope = span.makeCurrent()) {
							span.setAttribute("input.value", value);
							List<String> words = Arrays.asList(value.toLowerCase().split("\\W+"));
							span.setAttribute("words.count", words.size());
							return words;
						} catch (Exception e) {
							span.setStatus(StatusCode.ERROR);
							span.recordException(e);
							throw e;
						} finally {
							span.end();
						}
					})
					.map((key, value) -> {
						Span span = tracer.spanBuilder("map-to-keyvalue")
								.setParent(Context.current())
								.setSpanKind(SpanKind.INTERNAL)
								.startSpan();

						try (Scope scope = span.makeCurrent()) {
							span.setAttribute("word", value);
							return new KeyValue<>(value, value);
						} finally {
							span.end();
						}
					})
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(Duration.ofMillis(WINDOW_SIZE_MS)))
					.count(Materialized.as("WordCounts-1"))
					.toStream()
					.map((key, value) -> {
						Span span = tracer.spanBuilder("map-to-wordcount")
								.setParent(Context.current())
								.setSpanKind(SpanKind.INTERNAL)
								.startSpan();

						try (Scope scope = span.makeCurrent()) {
							span.setAttribute("word", key.key());
							span.setAttribute("count", value);
							span.setAttribute("window.start", key.window().start());
							span.setAttribute("window.end", key.window().end());

							return new KeyValue<>(null, new WordCount(
									key.key(),
									value,
									new Date(key.window().start()),
									new Date(key.window().end())
							));
						} finally {
							span.end();
						}
					});
		}
	}

	class TransformerWithTracing<K, V> implements Transformer<K, V, KeyValue<K, V>> {
		private final Tracer tracer;
		private ProcessorContext context;

		public TransformerWithTracing(Tracer tracer) {
			this.tracer = tracer;
		}

		@Override
		public void init(ProcessorContext context) {
			this.context = context;
		}

		@Override
		public KeyValue<K, V> transform(K key, V value) {
			Span span = tracer.spanBuilder("kafka-streams-process")
					.setSpanKind(SpanKind.CONSUMER)
					.startSpan();

			try (Scope scope = span.makeCurrent()) {
				span.setAttribute("kafka.topic", context.topic());
				span.setAttribute("kafka.partition", context.partition());
				span.setAttribute("kafka.offset", context.offset());
				if (key != null) {
					span.setAttribute("kafka.key", key.toString());
				}
				if (value != null) {
					span.setAttribute("kafka.value", value.toString());
				}

				return KeyValue.pair(key, value);
			} finally {
				span.end();
			}
		}

		@Override
		public void close() {
			// No resources to clean up
		}
	}

	static class WordCount {

		private String word;

		private long count;

		private Date start;

		private Date end;

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer("WordCount{");
			sb.append("word='").append(word).append('\'');
			sb.append(", count=").append(count);
			sb.append(", start=").append(start);
			sb.append(", end=").append(end);
			sb.append('}');
			return sb.toString();
		}

		WordCount() {

		}

		WordCount(String word, long count, Date start, Date end) {
			this.word = word;
			this.count = count;
			this.start = start;
			this.end = end;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public Date getStart() {
			return start;
		}

		public void setStart(Date start) {
			this.start = start;
		}

		public Date getEnd() {
			return end;
		}

		public void setEnd(Date end) {
			this.end = end;
		}
	}
}
