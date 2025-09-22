package io.mosip.registration.processor.reprocessor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import io.mosip.registration.processor.core.spi.restclient.RegistrationProcessorRestClientService;
import io.mosip.registration.processor.packet.storage.utils.PacketManagerService;
import io.mosip.registration.processor.reprocessor.verticle.ReprocessorVerticle;
import io.mosip.registration.processor.rest.client.service.impl.RegistrationProcessorRestClientServiceImpl;

/**
 * Spring configuration class for defining beans and configurations required by the Reprocessor Verticle.
 * <p>
 * This class is responsible for loading properties from {@code bootstrap.properties} and creating
 * Spring beans for the {@link ReprocessorVerticle}, {@link RegistrationProcessorRestClientService},
 * and {@link PacketManagerService}. These beans are used in the reprocessing workflow of the
 * MOSIP registration processor to handle packet reprocessing, REST client interactions, and packet
 * management.
 * </p>
 *
 * @author Pranav Kumar
 * @since 0.10.0
 */
@PropertySource("classpath:bootstrap.properties")
@Configuration
public class ReprocessorConfigBeans {

	/**
	 * Creates and configures a {@link ReprocessorVerticle} bean.
	 * <p>
	 * The {@link ReprocessorVerticle} is responsible for scheduling and executing the reprocessing
	 * logic for registration packets in the MOSIP system. This bean is used to instantiate the
	 * verticle that handles packet reprocessing workflows.
	 * </p>
	 *
	 * @return a new instance of {@link ReprocessorVerticle}
	 */
	@Bean
	public ReprocessorVerticle reprocessorVerticle() {
		return new ReprocessorVerticle();
	}

	/**
	 * Creates and configures a {@link RegistrationProcessorRestClientService} bean.
	 * <p>
	 * This bean provides a REST client service implementation for interacting with external APIs
	 * in the MOSIP registration processor. The {@link RegistrationProcessorRestClientServiceImpl}
	 * is used to make HTTP requests to various services during the reprocessing workflow.
	 * </p>
	 *
	 * @return a new instance of {@link RegistrationProcessorRestClientServiceImpl}
	 */
	@Bean
	public RegistrationProcessorRestClientService<Object> getRegistrationProcessorRestClientService() {
		return new RegistrationProcessorRestClientServiceImpl();
	}

	/**
	 * Creates and configures a {@link PacketManagerService} bean.
	 * <p>
	 * The {@link PacketManagerService} is responsible for managing packet-related operations, such
	 * as storage and retrieval, in the MOSIP registration processor. This service is utilized
	 * during the reprocessing of registration packets to handle packet data efficiently.
	 * </p>
	 *
	 * @return a new instance of {@link PacketManagerService}
	 */
	@Bean
	public PacketManagerService getPacketManagerService() {
		return new PacketManagerService();
	}
}
