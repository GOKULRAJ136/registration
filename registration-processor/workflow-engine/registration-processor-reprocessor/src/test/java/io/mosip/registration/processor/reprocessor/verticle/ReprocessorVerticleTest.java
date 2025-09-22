package io.mosip.registration.processor.reprocessor.verticle;

import io.mosip.registration.processor.core.abstractverticle.*;
import io.mosip.registration.processor.core.code.*;
import io.mosip.registration.processor.core.exception.*;
import io.mosip.registration.processor.core.http.ResponseWrapper;
import io.mosip.registration.processor.core.logger.LogDescription;
import io.mosip.registration.processor.core.spi.eventbus.EventHandler;
import io.mosip.registration.processor.core.spi.restclient.RegistrationProcessorRestClientService;
import io.mosip.registration.processor.rest.client.audit.builder.AuditLogRequestBuilder;
import io.mosip.registration.processor.rest.client.audit.dto.AuditResponseDto;
import io.mosip.registration.processor.status.code.*;
import io.mosip.registration.processor.status.dto.*;
import io.mosip.registration.processor.status.exception.TablenotAccessibleException;
import io.mosip.registration.processor.status.service.RegistrationStatusService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.env.Environment;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue; // Added import
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ReprocessorVerticleTest {

	@InjectMocks
	private ReprocessorVerticle reprocessorVerticle = new ReprocessorVerticle() {
		@Override
		public MosipEventBus getEventBus(Object verticleName, String url, int instanceNumber) {
			vertx = Vertx.vertx();

			return new MosipEventBus() {

				@Override
				public Vertx getEventbus() {
					return vertx;
				}

				@Override
				public void consume(MessageBusAddress fromAddress, EventHandler<EventDTO, Handler<AsyncResult<MessageDTO>>> eventHandler) {
				}

				@Override
				public void consumeAndSend(MessageBusAddress fromAddress, MessageBusAddress toAddress, EventHandler<EventDTO, Handler<AsyncResult<MessageDTO>>> eventHandler) {
				}

				@Override
				public void send(MessageBusAddress toAddress, MessageDTO message) {
				}

				@Override
				public void consumerHealthCheck(Handler<HealthCheckDTO> eventHandler, String address) {
				}

				@Override
				public void senderHealthCheck(Handler<HealthCheckDTO> eventHandler, String address) {
				}
			};
		}

		@Override
		public void send(MosipEventBus mosipEventBus, MessageBusAddress toAddress, MessageDTO message) {
		}
	};

	@Mock
	private RegistrationStatusService<String, InternalRegistrationStatusDto, RegistrationStatusDto> registrationStatusService;

	@Mock
	private AuditLogRequestBuilder auditLogRequestBuilder;

	@Mock
	private Environment environment;

	@Mock
	private LogDescription description;

	@Mock
	private RegistrationProcessorRestClientService<Object> registrationProcessorRestClientService;

	private MessageDTO dto = new MessageDTO();

	@Before
	public void setup() throws ApisResourceAccessException {
		// Mock Environment
		// n(environment.getProperty(anyString())).thenReturn("*");

		// Mock LogDescription
		// doNothing().when(description).setCode(anyString());
		// doNothing().when(description).setMessage(anyString());
		// when(description.getCode()).thenReturn("CODE");
		// when(description.getMessage()).thenReturn("MESSAGE");

		// Mock AuditLogRequestBuilder
		// ResponseWrapper<AuditResponseDto> responseWrapper = new ResponseWrapper<>();
		// responseWrapper.setResponse(new AuditResponseDto());
		// when(auditLogRequestBuilder.createAuditRequestBuilder(anyString(), anyString(), anyString(), anyString(), anyString(), any())).thenReturn(responseWrapper);

		// Mock RegistrationProcessorRestClientService
		// ReflectionTestUtils.setField(auditLogRequestBuilder, "registrationProcessorRestService", registrationProcessorRestClientService);
		// when(registrationProcessorRestClientService.postApi(any(), any(), any(), any(), any())).thenReturn(responseWrapper);

		// Set injected fields
		ReflectionTestUtils.setField(reprocessorVerticle, "cacheTargetSize", 200);
		ReflectionTestUtils.setField(reprocessorVerticle, "fetchSize", 2);
		ReflectionTestUtils.setField(reprocessorVerticle, "prefetchMultiplier", 1);
		ReflectionTestUtils.setField(reprocessorVerticle, "elapseTime", 21600L);
		ReflectionTestUtils.setField(reprocessorVerticle, "reprocessCount", 3);
		ReflectionTestUtils.setField(reprocessorVerticle, "reprocessExcludeStageNames", new ArrayList<>());
		ReflectionTestUtils.setField(reprocessorVerticle, "reprocessRestartTriggerFilter",
				new ArrayList<>(List.of("DemodedupStage:Success", "BioDedupeStage:*", "UinGeneratorStage:reprocess", "BioDedupeStage:reprocess")));
		ReflectionTestUtils.setField(reprocessorVerticle, "reprocessRestartFromStage", "SecurezoneNotificationStage");
		ReflectionTestUtils.setField(reprocessorVerticle, "environment", environment);

		// Initialize cache
		reprocessorVerticle.init();

		// Mock updateRegistrationStatusForWorkflowEngine
		doNothing().when(registrationStatusService).updateRegistrationStatusForWorkflowEngine(any(), anyString(), anyString());
	}

	@Test
	public void testProcessValid() throws Exception {
		List<InternalRegistrationStatusDto> dtolist = new ArrayList<>();
		InternalRegistrationStatusDto registrationStatusDto = new InternalRegistrationStatusDto();
		registrationStatusDto.setRegistrationId("2018701130000410092018110735");
		registrationStatusDto.setRegistrationType(RegistrationType.NEW.toString());
		registrationStatusDto.setRegistrationStageName("PacketValidatorStage");
		registrationStatusDto.setDefaultResumeAction("RESUME_PROCESSING");
		registrationStatusDto.setResumeTimeStamp(LocalDateTime.now());
		registrationStatusDto.setReProcessRetryCount(0);
		registrationStatusDto.setLatestTransactionStatusCode(RegistrationTransactionStatusCode.REPROCESS.toString());
		dtolist.add(registrationStatusDto);
		InternalRegistrationStatusDto registrationStatusDto1 = new InternalRegistrationStatusDto();
		registrationStatusDto1.setRegistrationId("2018701130000410092018110734");
		registrationStatusDto1.setRegistrationStageName("PacketValidatorStage");
		registrationStatusDto1.setReProcessRetryCount(1);
		registrationStatusDto1.setRegistrationType("NEW");
		registrationStatusDto1.setLatestTransactionStatusCode(RegistrationTransactionStatusCode.SUCCESS.toString());
		dtolist.add(registrationStatusDto1);

		when(registrationStatusService.getUnProcessedPackets(anyInt(), anyLong(), anyInt(), anyList(), anyList()))
				.thenReturn(dtolist);

		reprocessorVerticle.process(dto);
	}
	
	@Test
	public void testProcessFailure() throws Exception {
		List<InternalRegistrationStatusDto> dtolist = new ArrayList<>();
		InternalRegistrationStatusDto registrationStatusDto = new InternalRegistrationStatusDto();
		registrationStatusDto.setRegistrationId("2018701130000410092018110735");
		registrationStatusDto.setRegistrationStageName("PacketValidatorStage");
		registrationStatusDto.setDefaultResumeAction("RESUME_PROCESSING");
		registrationStatusDto.setResumeTimeStamp(LocalDateTime.now());
		registrationStatusDto.setRegistrationType("NEW");
		registrationStatusDto.setLatestTransactionStatusCode(RegistrationTransactionStatusCode.REPROCESS.toString());
		dtolist.add(registrationStatusDto);
		InternalRegistrationStatusDto registrationStatusDto1 = new InternalRegistrationStatusDto();
		registrationStatusDto1.setRegistrationId("2018701130000410092018110734");
		registrationStatusDto1.setRegistrationStageName("PacketValidatorStage");
		registrationStatusDto1.setReProcessRetryCount(3);
		registrationStatusDto1.setRegistrationType("NEW");
		registrationStatusDto1.setLatestTransactionStatusCode(RegistrationTransactionStatusCode.SUCCESS.toString());
		dtolist.add(registrationStatusDto1);

		when(registrationStatusService.getUnProcessedPackets(anyInt(), anyLong(), anyInt(), anyList(), anyList()))
				.thenReturn(dtolist);

		reprocessorVerticle.process(dto);
	}

	@Test
	public void exceptionTest() throws Exception {
		when(registrationStatusService.getUnProcessedPackets(anyInt(), anyLong(), anyInt(), anyList(), anyList()))
				.thenReturn(null);

		MessageDTO result = reprocessorVerticle.process(dto);
		assertEquals(null, result.getIsValid());
	}

	@Test
	public void nullPointerExceptionTest() throws Exception {
		when(registrationStatusService.getResumablePackets(anyInt()))
				.thenThrow(new NullPointerException("Test NPE"));

		MessageDTO result = reprocessorVerticle.process(dto);
		assertEquals(null, result.getIsValid());
	}

	@Test
	public void tableNotAccessibleExceptionTest() throws Exception {
		when(registrationStatusService.getUnProcessedPackets(anyInt(), anyLong(), anyInt(), anyList(), anyList()))
				.thenThrow(new TablenotAccessibleException("Table not accessible"));

		MessageDTO result = reprocessorVerticle.process(dto);
		assertEquals(null, result.getIsValid());
	}

	@Test
	public void testProcessValidWithResumablePackets() throws Exception {
		List<InternalRegistrationStatusDto> dtolist = new ArrayList<>();
		InternalRegistrationStatusDto registrationStatusDto = new InternalRegistrationStatusDto();
		registrationStatusDto.setRegistrationId("2018701130000410092018110735");
		registrationStatusDto.setRegistrationType(RegistrationType.NEW.toString());
		registrationStatusDto.setRegistrationStageName("PacketValidatorStage");
		registrationStatusDto.setDefaultResumeAction("RESUME_PROCESSING");
		registrationStatusDto.setResumeTimeStamp(LocalDateTime.now());
		registrationStatusDto.setReProcessRetryCount(0);
		registrationStatusDto.setLatestTransactionStatusCode(RegistrationTransactionStatusCode.REPROCESS.toString());
		dtolist.add(registrationStatusDto);
		List<InternalRegistrationStatusDto> reprocessorDtoList = new ArrayList<>();
		InternalRegistrationStatusDto registrationStatusDto1 = new InternalRegistrationStatusDto();
		registrationStatusDto1.setRegistrationId("2018701130000410092018110734");
		registrationStatusDto1.setRegistrationStageName("PacketValidatorStage");
		registrationStatusDto1.setReProcessRetryCount(1);
		registrationStatusDto1.setRegistrationType("NEW");
		registrationStatusDto1.setLatestTransactionStatusCode(RegistrationTransactionStatusCode.SUCCESS.toString());
		reprocessorDtoList.add(registrationStatusDto1);

		when(registrationStatusService.getResumablePackets(anyInt())).thenReturn(dtolist);
		when(registrationStatusService.getUnProcessedPackets(anyInt(), anyLong(), anyInt(), anyList(), anyList()))
				.thenReturn(reprocessorDtoList);

		reprocessorVerticle.process(dto);
	}

	@Test
	public void testProcessWithRestartFromStage() throws Exception {
		List<InternalRegistrationStatusDto> dtolist = new ArrayList<>();
		InternalRegistrationStatusDto registrationStatusDto = new InternalRegistrationStatusDto();

		registrationStatusDto.setRegistrationId("2018701130000410092018110735");
		registrationStatusDto.setRegistrationType(RegistrationType.NEW.toString());
		registrationStatusDto.setRegistrationStageName("BioDedupeStage");
		registrationStatusDto.setReProcessRetryCount(0);
		registrationStatusDto.setStatusCode(RegistrationStatusCode.PROCESSING.toString());
		registrationStatusDto.setLatestTransactionStatusCode(RegistrationTransactionStatusCode.REPROCESS.toString());
		dtolist.add(registrationStatusDto);

		when(registrationStatusService.getUnProcessedPackets(anyInt(), anyLong(), anyInt(), anyList(), anyList()))
				.thenReturn(dtolist);
		reprocessorVerticle.process(dto);

	}

}
