package io.mosip.registration.processor.reprocessor;

import io.mosip.registration.processor.core.config.reader.ConfigPropertyReader;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import io.mosip.registration.processor.reprocessor.verticle.ReprocessorVerticle;

/**
 * Main class for launching the Reprocessor Application in the MOSIP registration processor.
 * <p>
 * This class initializes the Spring application context, scans specified packages for configuration
 * and bean definitions, and deploys the {@link ReprocessorVerticle} to handle the reprocessing
 * of registration packets. It serves as the entry point for the reprocessor module, setting up
 * necessary configurations and starting the verticle for scheduling and executing reprocessing tasks.
 * </p>
 *
 * @author Pranav kumar
 * @since 0.10.0
 *
 */
public class ReprocessorApplication {

	/**
	 * Main method to launch the Reprocessor Application.
	 * <p>
	 * This method creates a Spring {@link AnnotationConfigApplicationContext}, scans multiple
	 * packages for configuration classes and beans, and refreshes the context to initialize
	 * the application. The scanned packages include core configurations, authentication adapter
	 * configurations (retrieved via {@link ConfigPropertyReader}), reprocessor configurations,
	 * status service configurations, kernel beans, and packet storage configurations. After
	 * context initialization, it retrieves the {@link ReprocessorVerticle} bean and invokes
	 * its {@code deployVerticle} method to start the reprocessing workflow.
	 * </p>
	 *
	 * @param args command-line arguments (not used)
	 */
	public static void main(String[] args) {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.scan("io.mosip.registration.processor.core.config",
				ConfigPropertyReader.getConfig("mosip.auth.adapter.impl.basepackage"),
				"io.mosip.registration.processor.reprocessor.config",
				"io.mosip.registration.processor.status.config",
				"io.mosip.registration.processor.core.kernel.beans",
				"io.mosip.registration.processor.packet.storage.config");
		ctx.refresh();

		ReprocessorVerticle reprocessorVerticle = ctx.getBean(ReprocessorVerticle.class);
		reprocessorVerticle.deployVerticle();
	}

}
