package io.mosip.registration.processor.reprocessor.constants;

/**
 * Constants used in the Reprocessor Verticle for configuration and scheduling.
 * <p>
 * This class defines static final constants used by the {@link io.mosip.registration.processor.reprocessor.verticle.ReprocessorVerticle}
 * for configuring message bus addresses, user identification, Ceylon scheduler settings, and cron expression properties.
 * These constants are used to standardize configuration keys and values for reprocessing registration packets in the MOSIP system.
 * </p>
 *
 * @author Pranav Kumar
 * @since 0.10.0
 */
public class ReprocessorConstants {

	/**
	 * Suffix for outbound message bus addresses.
	 */
	public static final String BUS_OUT = "-bus-out";

	/**
	 * Suffix for inbound message bus addresses.
	 */
	public static final String BUS_IN = "-bus-in";

	/**
	 * User identifier for system operations.
	 */
	public static final String USER = "MOSIP_SYSTEM";

	/**
	 * Version of the Ceylon scheduler library used for scheduling tasks.
	 * <p>
	 * Specifies the Ceylon Chime scheduler dependency ({@code ceylon:herd.schedule.chime/0.2.0})
	 * that is downloaded and unzipped from the artifactory when starting the Docker container.
	 * </p>
	 */
	public static final String CEYLON_SCHEDULER = "ceylon:herd.schedule.chime/0.2.0";

	/**
	 * Event name for the scheduler timer.
	 */
	public static final String TIMER_EVENT = "scheduler:stage_timer";

	/**
	 * Key for the cron expression type.
	 */
	public static final String TYPE = "type";

	/**
	 * Key for the seconds field in the cron expression.
	 */
	public static final String SECONDS = "seconds";

	/**
	 * Key for the minutes field in the cron expression.
	 */
	public static final String MINUTES = "minutes";

	/**
	 * Key for the hours field in the cron expression.
	 */
	public static final String HOURS = "hours";

	/**
	 * Key for the days of month field in the cron expression.
	 */
	public static final String DAY_OF_MONTH = "days of month";

	/**
	 * Key for the months field in the cron expression.
	 */
	public static final String MONTHS = "months";

	/**
	 * Key for the days of week field in the cron expression.
	 */
	public static final String DAYS_OF_WEEK = "days of week";

	/**
	 * Property key for the reprocessor cron expression type.
	 */
	public static final String TYPE_VALUE = "registration.processor.reprocess.type";

	/**
	 * Property key for the seconds value in the reprocessor cron expression.
	 */
	public static final String SECONDS_VALUE = "registration.processor.reprocess.seconds";

	/**
	 * Property key for the minutes value in the reprocessor cron expression.
	 */
	public static final String MINUTES_VALUE = "registration.processor.reprocess.minutes";

	/**
	 * Property key for the hours value in the reprocessor cron expression.
	 */
	public static final String HOURS_VALUE = "registration.processor.reprocess.hours";

	/**
	 * Property key for the days of month value in the reprocessor cron expression.
	 */
	public static final String DAY_OF_MONTH_VALUE = "registration.processor.reprocess.days_of_month";

	/**
	 * Property key for the months value in the reprocessor cron expression.
	 */
	public static final String MONTHS_VALUE = "registration.processor.reprocess.months";

	/**
	 * Property key for the days of week value in the reprocessor cron expression.
	 */
	public static final String DAYS_OF_WEEK_VALUE = "registration.processor.reprocess.days_of_week";

	/**
	 * Identifier for the Ceylon Chime scheduler.
	 */
	public static final String CHIME = "chime";

	/**
	 * Key for the scheduler operation type.
	 */
	public static final String OPERATION = "operation";

	/**
	 * Value for the scheduler operation, indicating a create action.
	 */
	public static final String OPERATION_VALUE = "create";

	/**
	 * Key for the scheduler task name.
	 */
	public static final String NAME = "name";

	/**
	 * Value for the scheduler task name, matching the timer event.
	 */
	public static final String NAME_VALUE = "scheduler:stage_timer";

	/**
	 * Key for the scheduler task description.
	 */
	public static final String DESCRIPTION = "description";

	/**
	 * Message indicating completion of the reprocessing task.
	 */
	public static final String REPROCESS_COMPLETE = "Reprocess Completed";
}
