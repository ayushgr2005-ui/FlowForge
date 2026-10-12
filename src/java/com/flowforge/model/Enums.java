package com.flowforge.model;

public class Enums {
    public enum ExecutionStatus { PENDING, RUNNING, COMPLETED, FAILED }
    public enum JobStatus { BLOCKED, READY, RUNNING, SUCCESS, FAILED, DEAD_LETTER }
    public enum JobType { LOG, COMPUTE, TRANSFORM, HTTP_DISPATCH }
}