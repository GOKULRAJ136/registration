package io.mosip.registration.processor.reprocessor.config;

import lombok.Data;

import java.util.List;

@Data
public class AllocationConfig {
    private List<String> processes;
    private List<String> statuses;
    private int percentageAllocation;
}