package br.com.dcc.springbatchexamples.configuration;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.support.ListItemReader;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import br.com.dcc.springbatchexamples.exception.ErrorHandlingException;
import br.com.dcc.springbatchexamples.listener.SimpleJobListener;
import br.com.dcc.springbatchexamples.processor.ErrorHandlingRetryItemProcessor;
import br.com.dcc.springbatchexamples.writer.ErrorHandlingRetryItemWriter;




@Configuration
public class ErrorHandlingSkipConfiguration {

	@Bean
	public ListItemReader<String> errorHandlingSkipReader() {

		List<String> items = new ArrayList<>();

		for (int i = 0; i < 100; i++) {
			items.add(String.valueOf(i));
		}

		return new ListItemReader<>(items);

	}

	@Bean
	@StepScope
	public ErrorHandlingRetryItemProcessor errorHandlingSkipItemProcessor(@Value("#{jobParameters['skip']}") String skip) {
		ErrorHandlingRetryItemProcessor errorHandlingSkipItemProcessor = new ErrorHandlingRetryItemProcessor();
		errorHandlingSkipItemProcessor.setRetry(StringUtils.hasText(skip) && skip.equalsIgnoreCase("processor"));
		return errorHandlingSkipItemProcessor;
	}

	@Bean
	@StepScope
	public ErrorHandlingRetryItemWriter errorHandlingSkipWriter(@Value("#{jobParameters['skip']}") String skip) {
		ErrorHandlingRetryItemWriter errorHandlingSkipItemWriter = new ErrorHandlingRetryItemWriter();
		errorHandlingSkipItemWriter.setRetry(StringUtils.hasText(skip) && skip.equalsIgnoreCase("writer"));
		return errorHandlingSkipItemWriter;
	}

	@Bean
	public Step errorHandlingSkipStep1(StepBuilderFactory stepBuilderFactory) {
		return stepBuilderFactory.get("ErrorHandlingSkipStep1")
				.<String, String>chunk(10)
				.reader(errorHandlingSkipReader())
				.processor(errorHandlingSkipItemProcessor(null))
				.writer(errorHandlingSkipWriter(null))
				.faultTolerant()
				.skip(ErrorHandlingException.class)
				.skipLimit(15)
				.build();
	}

	@Bean
	public Job errorHandlingSkipJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
		return jobBuilderFactory.get("ErrorHandlingSkipJob")
				.start(errorHandlingSkipStep1(stepBuilderFactory))
				.listener(new SimpleJobListener())
				.build();

	}

}
