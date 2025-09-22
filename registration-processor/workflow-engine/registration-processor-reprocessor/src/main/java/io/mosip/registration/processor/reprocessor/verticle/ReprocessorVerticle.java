package io.mosip.registration.processor.reprocessor.verticle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.core.abstractverticle.MessageBusAddress;
import io.mosip.registration.processor.core.abstractverticle.MessageDTO;
import io.mosip.registration.processor.core.abstractverticle.MosipEventBus;
import io.mosip.registration.processor.core.abstractverticle.MosipRouter;
import io.mosip.registration.processor.core.abstractverticle.MosipVerticleAPIManager;
import io.mosip.registration.processor.core.code.EventId;
import io.mosip.registration.processor.core.code.EventName;
import io.mosip.registration.processor.core.code.EventType;
import io.mosip.registration.processor.core.code.ModuleName;
import io.mosip.registration.processor.core.code.RegistrationTransactionStatusCode;
import io.mosip.registration.processor.core.code.RegistrationTransactionTypeCode;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.exception.util.PlatformErrorMessages;
import io.mosip.registration.processor.core.exception.util.PlatformSuccessMessages;
import io.mosip.registration.processor.core.logger.LogDescription;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.mosip.registration.processor.core.status.util.StatusUtil;
import io.mosip.registration.processor.core.util.MessageBusUtil;
import io.mosip.registration.processor.reprocessor.constants.ReprocessorConstants;
import io.mosip.registration.processor.rest.client.audit.builder.AuditLogRequestBuilder;
import io.mosip.registration.processor.status.code.RegistrationStatusCode;
import io.mosip.registration.processor.status.dto.InternalRegistrationStatusDto;
import io.mosip.registration.processor.status.dto.RegistrationStatusDto;
import io.mosip.registration.processor.status.exception.TablenotAccessibleException;
import io.mosip.registration.processor.status.service.RegistrationStatusService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

/**
 * The Reprocessor Verticle is responsible for deploying a scheduler and implementing the re-processing logic
 * for registration packets in the MOSIP system. It periodically fetches packets that are eligible for reprocessing
 * (e.g., resumable or unprocessed packets based on elapsed time and retry counts) from the database, updates their
 * status, and reinjects them into the message bus for further processing.
 *
 * <p>This verticle uses a cron-based scheduler to trigger reprocessing at configurable intervals. It handles
 * scenarios where packets need to be restarted from a specific stage based on trigger filters. To optimize
 * performance and reduce database query latency (which can take 2-5 seconds per fetch), an in-memory cache
 * is introduced to prefetch and store excess eligible packets. This cache is refilled opportunistically during
 * each reprocessing cycle, smoothing out the reprocessing curve and saving time on frequent DB interactions.</p>
 *
 * <p>Key configurations:
 * <ul>
 *     <li>{@code registration.processor.reprocess.fetchsize}: Number of packets to process per cycle (e.g., 100).</li>
 *     <li>{@code registration.processor.reprocess.elapse.time}: Elapsed time threshold for unprocessed packets.</li>
 *     <li>{@code registration.processor.reprocess.attempt.count}: Maximum reprocess attempts before failing a packet.</li>
 *     <li>{@code mosip.registration.processor.reprocessor.exclude-stage-names}: Stages to exclude from reprocessing.</li>
 *     <li>{@code registration.processor.reprocess.restart-from-stage}: Stage to restart from if triggers match.</li>
 *     <li>{@code registration.processor.reprocess.restart-trigger-filter}: Filters for restarting from a stage (e.g., "stageName:STATUS").</li>
 *     <li>{@code registration.processor.reprocess.cache.prefetch-multiplier}: Multiplier for prefetching extra packets into cache (default: 1, meaning fetch extra equal to fetchSize).</li>
 * </ul>
 * </p>
 *
 * <p>The cache is implemented as a thread-safe {@link java.util.concurrent.LinkedBlockingQueue} to handle concurrent
 * access in the Vert.x event-driven environment. Packets are added to the cache only if they are eligible for
 * reprocessing, ensuring cache integrity.</p>
 *
 * @author Alok Ranjan
 * @author Sowmya
 * @author Pranav Kumar
 * @since 0.10.0
 */
@Component
public class ReprocessorVerticle extends MosipVerticleAPIManager {

	private static final Logger LOGGER = RegProcessorLogger.getLogger(ReprocessorVerticle.class);


	private static final String VERTICLE_PROPERTY_PREFIX = "mosip.regproc.reprocessor.";

	/**
	 * The cluster manager URL used for Vert.x clustering.
	 */
	@Value("${vertx.cluster.configuration}")
	private String clusterManagerUrl;

	/**
	 * The Spring environment for accessing configuration properties.
	 */
	@Autowired
	private Environment environment;

	/**
	 * The MosipEventBus instance for sending messages to other stages.
	 */
	private MosipEventBus mosipEventBus = null;

	/**
	 * The number of packets to fetch and process per reprocessing cycle.
	 */
	@Value("${registration.processor.reprocess.fetchsize:200}")
	private Integer fetchSize;

	/**
	 * The target number of packets to maintain in the cache (e.g., 1000 or 2000).
	 */
	@Value("${registration.processor.reprocess.cache.target.size:2000}")
	private Integer cacheTargetSize;

	/**
	 * The elapsed time (in milliseconds) after which unprocessed packets are eligible for reprocessing.
	 */
	@Value("${registration.processor.reprocess.elapse.time}")
	private long elapseTime;

	/**
	 * The maximum number of reprocess attempts for a packet before marking it as failed.
	 */
	@Value("${registration.processor.reprocess.attempt.count}")
	private Integer reprocessCount;

	/**
	 * Comman seperated stage names that should be excluded while reprocessing.
	 */
	@Value("#{T(java.util.Arrays).asList('${mosip.registration.processor.reprocessor.exclude-stage-names:PacketReceiverStage}')}")
	private List<String> reprocessExcludeStageNames;

	/**
	 * The stage name to restart processing from if restart triggers are met.
	 */
	@Value("${registration.processor.reprocess.restart-from-stage}")
	private String reprocessRestartFromStage;

	/**
	 * List of restart trigger filters in the format "stageName:status" (e.g., "validationStage:SUCCESS").
	 * Use "*" for status to match any of SUCCESS, IN_PROGRESS, REPROCESS.
	 */
	@Value("#{'${registration.processor.reprocess.restart-trigger-filter}'.split(',')}")
	private List<String> reprocessRestartTriggerFilter;

	/**
	 * Multiplier for prefetching extra packets into the cache beyond the current fetchSize.
	 * For example, a value of 1 means prefetch an additional fetchSize packets (total 2x).
	 * Default: 1.
	 */
	@Value("${registration.processor.reprocess.cache.prefetch-multiplier:1}")
	private Integer prefetchMultiplier;

	/**
	 * The in-memory cache for prefetching eligible packets to reduce DB query frequency.
	 * Implemented as a thread-safe queue with a capacity limit to prevent memory overflow.
	 */
	private BlockingQueue<InternalRegistrationStatusDto> packetCache; // Adjustable capacity

	/**
	 * Flag indicating if the last transaction was successful.
	 */
	private boolean isTransactionSuccessful;

	/**
	 * Service for managing registration status and fetching eligible packets.
	 */
	@Autowired
	private RegistrationStatusService<String, InternalRegistrationStatusDto, RegistrationStatusDto> registrationStatusService;

	/**
	 * Builder for creating audit log requests.
	 */
	@Autowired
	private AuditLogRequestBuilder auditLogRequestBuilder;

	/**
	 * Mosip router for handling API routes.
	 */
	@Autowired
	private MosipRouter router;

	/**
	 * The server port for the verticle.
	 */
	@Value("${server.port}")
	private String port;

	/**
	 * Initializes the cache after dependencies are injected.
	 */
	@PostConstruct
	public void init() {
		this.packetCache = new LinkedBlockingQueue<>(cacheTargetSize * 2);
	}

	/**
	 * Deploys the verticle, initializes the event bus, and deploys the scheduler.
	 */
	public void deployVerticle() {
		mosipEventBus = this.getEventBus(this, clusterManagerUrl);
		deployScheduler(getVertx());
	}

	/**
	 * Deploys the Ceylon scheduler verticle for triggering reprocessing at intervals.
	 *
	 * @param vertx the Vert.x instance.
	 */
	private void deployScheduler(Vertx vertx) {
		vertx.deployVerticle(ReprocessorConstants.CEYLON_SCHEDULER, this::schedulerResult);
	}

	/**
	 * Callback handler for scheduler deployment result.
	 *
	 * @param res the asynchronous deployment result.
	 */
	public void schedulerResult(AsyncResult<String> res) {
		if (res.succeeded()) {
			LOGGER.info("ReprocessorVerticle::schedular()::deployed");
			cronScheduling(vertx);
		} else {
			LOGGER.error("ReprocessorVerticle::schedular()::deployment failure " + res.cause().getMessage());
		}
	}

	/**
	 * Configures cron scheduling based on properties from the environment.
	 * Sets up an event bus consumer to listen for timer events and trigger processing.
	 *
	 * @param vertx the Vert.x instance.
	 */
	private void cronScheduling(Vertx vertx) {
		EventBus eventBus = vertx.eventBus();
		// listen the timer events
		eventBus.consumer((ReprocessorConstants.TIMER_EVENT), message -> {
			process(new MessageDTO());
		});

		// Timer description from config
		JsonObject timer = (new JsonObject())
				.put(ReprocessorConstants.TYPE, environment.getProperty(ReprocessorConstants.TYPE_VALUE))
				.put(ReprocessorConstants.SECONDS, environment.getProperty(ReprocessorConstants.SECONDS_VALUE))
				.put(ReprocessorConstants.MINUTES, environment.getProperty(ReprocessorConstants.MINUTES_VALUE))
				.put(ReprocessorConstants.HOURS, environment.getProperty(ReprocessorConstants.HOURS_VALUE))
				.put(ReprocessorConstants.DAY_OF_MONTH, environment.getProperty(ReprocessorConstants.DAY_OF_MONTH_VALUE))
				.put(ReprocessorConstants.MONTHS, environment.getProperty(ReprocessorConstants.MONTHS_VALUE))
				.put(ReprocessorConstants.DAYS_OF_WEEK, environment.getProperty(ReprocessorConstants.DAYS_OF_WEEK_VALUE));

		// Send create scheduler message
		eventBus.send(ReprocessorConstants.CHIME,
				(new JsonObject()).put(ReprocessorConstants.OPERATION, ReprocessorConstants.OPERATION_VALUE)
						.put(ReprocessorConstants.NAME, ReprocessorConstants.NAME_VALUE)
						.put(ReprocessorConstants.DESCRIPTION, timer),
				ar -> {
					if (ar.succeeded()) {
						LOGGER.info(LoggerFileConstant.SESSIONID.toString(),
								LoggerFileConstant.REGISTRATIONID.toString(), "", "ReprocessorVerticle::schedular()::started");
					} else {
						LOGGER.error(LoggerFileConstant.SESSIONID.toString(),
								LoggerFileConstant.REGISTRATIONID.toString(), "", "ReprocessorVerticle::schedular()::failed " + ar.cause());
						vertx.close();
					}
				});
	}

	/**
	 * Sends a message DTO to the specified message bus address.
	 *
	 * @param message the MessageDTO to send.
	 * @param toAddress the target MessageBusAddress.
	 */
	public void sendMessage(MessageDTO message, MessageBusAddress toAddress) {
		this.send(this.mosipEventBus, toAddress, message);
	}

	/**
	 * Starts the verticle by setting up routes and creating the server.
	 */
	@Override
	public void start() {
		router.setRoute(this.postUrl(getVertx(), null, null));
		this.createServer(router.getRouter(), Integer.parseInt(port));
	}

	/**
	 * Processes the reprocessing logic for eligible packets. This method is triggered by the scheduler.
	 * It first attempts to fulfill the fetchSize from the in-memory cache. If the cache does not have enough,
	 * it queries the database for resumable and unprocessed packets, processing up to fetchSize and caching
	 * any excess (based on prefetchMultiplier) for future cycles. This reduces DB access latency.
	 *
	 * <p>Handles packet status updates, message bus injection, and audit logging. If retry count is exhausted,
	 * marks packets as REPROCESS_FAILED.</p>
	 *
	 * @param object the input MessageDTO (not used directly but required by interface).
	 * @return the processed MessageDTO with error flags if applicable.
	 */
	@Override
	public MessageDTO process(MessageDTO object) {
		List<InternalRegistrationStatusDto> reprocessorDtoList = new ArrayList<>();
		LogDescription description = new LogDescription();
		List<String> statusList = new ArrayList<>();
		statusList.add(RegistrationTransactionStatusCode.SUCCESS.toString());
		statusList.add(RegistrationTransactionStatusCode.REPROCESS.toString());
		statusList.add(RegistrationTransactionStatusCode.IN_PROGRESS.toString());
		LOGGER.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(), "", "ReprocessorVerticle::process()::entry");

		StringBuffer ridSb = new StringBuffer();
		try {
			Map<String, Set<String>> reprocessRestartTriggerMap = initializeReprocessRestartTriggerMapping();

			// Step 1: Drain from cache first (up to fetchSize)
			int drained = packetCache.drainTo(reprocessorDtoList, fetchSize);
			int remainingToFetch = fetchSize - drained;

			// Step 2: If cache didn't fulfill, query DB for more (including prefetch)
			if (remainingToFetch > 0) {
				int querySize = remainingToFetch + (fetchSize * prefetchMultiplier);
				List<InternalRegistrationStatusDto> fetchedFromDb = registrationStatusService.getResumablePackets(querySize);
				if (!CollectionUtils.isEmpty(fetchedFromDb)) {
					if (fetchedFromDb.size() < querySize) {
						List<InternalRegistrationStatusDto> unprocessed = registrationStatusService.getUnProcessedPackets(
								querySize - fetchedFromDb.size(), elapseTime, reprocessCount, statusList, reprocessExcludeStageNames);
						if (!CollectionUtils.isEmpty(unprocessed)) {
							fetchedFromDb.addAll(unprocessed);
						}
					}
				} else {
					fetchedFromDb = registrationStatusService.getUnProcessedPackets(querySize, elapseTime,
							reprocessCount, statusList, reprocessExcludeStageNames);
				}

				// Add to processing list up to remainingToFetch, cache the rest
				for (InternalRegistrationStatusDto dto : fetchedFromDb) {
					if (reprocessorDtoList.size() < fetchSize) {
						reprocessorDtoList.add(dto);
					} else {
						packetCache.offer(dto); // Add excess to cache
					}
				}
			}

			// Step 3: Process the collected list
			if (!CollectionUtils.isEmpty(reprocessorDtoList)) {
				reprocessorDtoList.forEach(dto -> {
					String registrationId = dto.getRegistrationId();
					ridSb.append(registrationId).append(",");
					MessageDTO messageDTO = new MessageDTO();
					messageDTO.setRid(registrationId);
					messageDTO.setReg_type(dto.getRegistrationType());
					messageDTO.setSource(dto.getSource());
					messageDTO.setIteration(dto.getIteration());
					messageDTO.setWorkflowInstanceId(dto.getWorkflowInstanceId());

					if (reprocessCount.equals(dto.getReProcessRetryCount())) {
						dto.setLatestTransactionStatusCode(RegistrationTransactionStatusCode.REPROCESS_FAILED.toString());
						dto.setLatestTransactionTypeCode(RegistrationTransactionTypeCode.PACKET_REPROCESS.toString());
						dto.setStatusComment(StatusUtil.RE_PROCESS_FAILED.getMessage());
						dto.setStatusCode(RegistrationStatusCode.REPROCESS_FAILED.toString());
						dto.setSubStatusCode(StatusUtil.RE_PROCESS_FAILED.getCode());
						messageDTO.setIsValid(false);
						description.setMessage(PlatformSuccessMessages.RPR_RE_PROCESS_FAILED.getMessage());
						description.setCode(PlatformSuccessMessages.RPR_RE_PROCESS_FAILED.getCode());
					} else {
						messageDTO.setIsValid(true);
						isTransactionSuccessful = true;
						String stageName;
						if (isRestartFromStageRequired(dto, reprocessRestartTriggerMap)) {
							stageName = MessageBusUtil.getMessageBusAdress(reprocessRestartFromStage).concat(ReprocessorConstants.BUS_IN);
							sendAndSetStatus(dto, messageDTO, stageName);
							dto.setStatusComment(StatusUtil.RE_PROCESS_RESTART_FROM_STAGE.getMessage());
							dto.setSubStatusCode(StatusUtil.RE_PROCESS_RESTART_FROM_STAGE.getCode());
							description.setMessage(PlatformSuccessMessages.RPR_SENT_TO_REPROCESS_RESTART_FROM_STAGE_SUCCESS.getMessage());
							description.setCode(PlatformSuccessMessages.RPR_SENT_TO_REPROCESS_RESTART_FROM_STAGE_SUCCESS.getCode());
						} else {
							stageName = MessageBusUtil.getMessageBusAdress(dto.getRegistrationStageName());
							if (RegistrationTransactionStatusCode.SUCCESS.name().equalsIgnoreCase(dto.getLatestTransactionStatusCode())) {
								stageName = stageName.concat(ReprocessorConstants.BUS_OUT);
							} else {
								stageName = stageName.concat(ReprocessorConstants.BUS_IN);
							}
							sendAndSetStatus(dto, messageDTO, stageName);
							dto.setStatusComment(StatusUtil.RE_PROCESS_COMPLETED.getMessage());
							dto.setSubStatusCode(StatusUtil.RE_PROCESS_COMPLETED.getCode());
							description.setMessage(PlatformSuccessMessages.RPR_SENT_TO_REPROCESS_SUCCESS.getMessage());
							description.setCode(PlatformSuccessMessages.RPR_SENT_TO_REPROCESS_SUCCESS.getCode());
						}
					}

					LOGGER.info(LoggerFileConstant.SESSIONID.toString(),
							LoggerFileConstant.REGISTRATIONID.toString(), registrationId, description.getMessage());

					String moduleId = PlatformSuccessMessages.RPR_SENT_TO_REPROCESS_SUCCESS.getCode();
					String moduleName = ModuleName.RE_PROCESSOR.toString();
					registrationStatusService.updateRegistrationStatusForWorkflowEngine(dto, moduleId, moduleName);

					String eventId = EventId.RPR_402.toString();
					String eventName = EventName.UPDATE.toString();
					String eventType = EventType.BUSINESS.toString();

					if (!isTransactionSuccessful) {
						auditLogRequestBuilder.createAuditRequestBuilder(description.getMessage(), eventId, eventName,
								eventType, moduleId, moduleName, registrationId);
					}
				});
			}
		} catch (TablenotAccessibleException e) {
			isTransactionSuccessful = false;
			object.setInternalError(Boolean.TRUE);
			description.setMessage(PlatformErrorMessages.RPR_RGS_REGISTRATION_TABLE_NOT_ACCESSIBLE.getMessage());
			description.setCode(PlatformErrorMessages.RPR_RGS_REGISTRATION_TABLE_NOT_ACCESSIBLE.getCode());
			LOGGER.error(LoggerFileConstant.SESSIONID.toString(), description.getCode() + " -- ",
					PlatformErrorMessages.RPR_RGS_REGISTRATION_TABLE_NOT_ACCESSIBLE.getMessage(), e.toString());
		} catch (Exception ex) {
			isTransactionSuccessful = false;
			description.setMessage(PlatformErrorMessages.REPROCESSOR_VERTICLE_FAILED.getMessage());
			description.setCode(PlatformErrorMessages.REPROCESSOR_VERTICLE_FAILED.getCode());
			LOGGER.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					description.getCode() + " -- ", PlatformErrorMessages.REPROCESSOR_VERTICLE_FAILED.getMessage() + ex.getMessage()
							+ ExceptionUtils.getStackTrace(ex));
			object.setInternalError(Boolean.TRUE);
		} finally {
			LOGGER.info(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					null, description.getMessage());
			if (isTransactionSuccessful) {
				description.setMessage(PlatformSuccessMessages.RPR_RE_PROCESS_SUCCESS.getMessage());
			}

			String eventId = isTransactionSuccessful ? EventId.RPR_402.toString() : EventId.RPR_405.toString();
			String eventName = isTransactionSuccessful ? EventName.UPDATE.toString() : EventName.EXCEPTION.toString();
			String eventType = isTransactionSuccessful ? EventType.BUSINESS.toString() : EventType.SYSTEM.toString();

			String moduleId = isTransactionSuccessful ? PlatformSuccessMessages.RPR_RE_PROCESS_SUCCESS.getCode() : description.getCode();
			String moduleName = ModuleName.RE_PROCESSOR.toString();
			auditLogRequestBuilder.createAuditRequestBuilder(description.getMessage(), eventId, eventName, eventType,
					moduleId, moduleName, (ridSb.length() > 1 ? ridSb.substring(0, ridSb.length() - 1) : ""));
		}

		return object;
	}

	/**
	 * Initializes a map for restart triggers based on configured filters.
	 *
	 * @return a map where keys are stage names and values are sets of transaction status codes that trigger a restart.
	 */
	private Map<String, Set<String>> initializeReprocessRestartTriggerMapping() {
		Map<String, Set<String>> reprocessRestartTriggerMap = new HashMap<String, Set<String>>();
		for (String filter : reprocessRestartTriggerFilter) {
			String[] stageAndStatus = filter.split(":");
			String stageName = stageAndStatus[0];
			String latestTransactionStatusCode = stageAndStatus[1];

			Set<String> latestTransactionStatusCodeSet = reprocessRestartTriggerMap.computeIfAbsent(stageName, k -> new HashSet<>());
			if (latestTransactionStatusCode.equalsIgnoreCase("*")) {
				latestTransactionStatusCodeSet.add(RegistrationTransactionStatusCode.SUCCESS.toString());
				latestTransactionStatusCodeSet.add(RegistrationTransactionStatusCode.IN_PROGRESS.toString());
				latestTransactionStatusCodeSet.add(RegistrationTransactionStatusCode.REPROCESS.toString());
			} else {
				latestTransactionStatusCodeSet.add(latestTransactionStatusCode.toUpperCase());
			}
		}
		return reprocessRestartTriggerMap;
	}

	private void setReprocessRestartTriggerMap(Map<String, Set<String>> reprocessRestartTriggerMap, String stageName,
											   String latestTransactionStatusCode, Set<String> latestTransactionStatusCodeSet) {
		if (latestTransactionStatusCode.equalsIgnoreCase("*")) {
			latestTransactionStatusCodeSet.add(RegistrationTransactionStatusCode.SUCCESS.toString());
			latestTransactionStatusCodeSet.add(RegistrationTransactionStatusCode.IN_PROGRESS.toString());
			latestTransactionStatusCodeSet.add(RegistrationTransactionStatusCode.REPROCESS.toString());
		} else {
			latestTransactionStatusCodeSet.add(latestTransactionStatusCode.toUpperCase());
		}
		reprocessRestartTriggerMap.put(stageName, latestTransactionStatusCodeSet);
	}

	/**
	 * Checks if a packet requires restarting from a specific stage based on the trigger map.
	 *
	 * @param dto the registration status DTO.
	 * @param reprocessRestartTriggerMap the trigger map.
	 * @return true if restart is required, false otherwise.
	 */
	private boolean isRestartFromStageRequired(InternalRegistrationStatusDto dto,
											   Map<String, Set<String>> reprocessRestartTriggerMap) {
		String stageName = dto.getRegistrationStageName();
		Set<String> latestTransactionStatusCodes = reprocessRestartTriggerMap.get(stageName);
		return latestTransactionStatusCodes != null && latestTransactionStatusCodes.contains(dto.getLatestTransactionStatusCode());
	}

	/**
	 * Sends the message DTO to the bus and updates the DTO status for reprocessing.
	 *
	 * @param dto the registration status DTO.
	 * @param messageDTO the message DTO.
	 * @param stageName the target stage name for the message bus.
	 */
	private void sendAndSetStatus(InternalRegistrationStatusDto dto, MessageDTO messageDTO, String stageName) {
		MessageBusAddress address = new MessageBusAddress(stageName);
		sendMessage(messageDTO, address);
		dto.setUpdatedBy(ReprocessorConstants.USER);
		Integer reprocessRetryCount = dto.getReProcessRetryCount() != null ? dto.getReProcessRetryCount() + 1 : 1;
		dto.setReProcessRetryCount(reprocessRetryCount);
		dto.setLatestTransactionStatusCode(RegistrationTransactionStatusCode.REPROCESS.toString());
		dto.setLatestTransactionTypeCode(RegistrationTransactionTypeCode.PACKET_REPROCESS.toString());
	}

	/**
	 * Returns the property prefix for this verticle.
	 *
	 * @return the prefix string.
	 */
	@Override
	protected String getPropertyPrefix() {
		return VERTICLE_PROPERTY_PREFIX;
	}
}
