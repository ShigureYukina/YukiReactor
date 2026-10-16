package com.yuki.reactor.observability.dto;

/** Kind of moment placed on a task timeline. */
public enum TimelineEventType {

    /** The logical task was accepted and persisted. */
    TASK_CREATED,

    /** An attempt claimed the task and started running. */
    ATTEMPT_STARTED,

    /** An attempt reported an outcome, or was closed by the consistency sweep. */
    ATTEMPT_FINISHED,

    /** A scheduling or control decision was recorded about this task. */
    DECISION
}
