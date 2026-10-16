package com.yuki.reactor.core.handler;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.yuki.reactor.core.exception.UnknownTaskTypeException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskHandlerRegistryTest {

    @Test
    void resolvesHandlersCaseInsensitively() {
        TaskHandler echo = handler("echo");
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(echo));

        assertThat(registry.knows("echo")).isTrue();
        assertThat(registry.knows("ECHO")).isTrue();
        assertThat(registry.knows(" Echo ")).isTrue();
        assertThat(registry.require("EcHo")).isSameAs(echo);
        assertThat(registry.registeredTypes()).containsExactly("echo");
    }

    @Test
    void rejectsDuplicateTaskTypesAtStartup() {
        TaskHandler first = handler("Report");
        TaskHandler second = handler("report");

        assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(first, second)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate task type");
    }

    @Test
    void rejectsBlankTaskTypesAtStartup() {
        assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(handler(" "))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("blank task type");
    }

    @Test
    void unknownTaskTypeIsRejectedInsteadOfReturningNull() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(handler("echo")));

        assertThat(registry.knows("missing")).isFalse();
        assertThatThrownBy(() -> registry.require("missing"))
                .isInstanceOf(UnknownTaskTypeException.class)
                .hasMessageContaining("missing");
        assertThatThrownBy(() -> registry.require(null))
                .isInstanceOf(UnknownTaskTypeException.class);
    }

    @Test
    void emptyRegistryIsUsableButKnowsNothing() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of());

        assertThat(registry.registeredTypes()).isEmpty();
        assertThat(registry.knows("echo")).isFalse();
    }

    private static TaskHandler handler(String taskType) {
        return new TaskHandler() {
            @Override
            public String taskType() {
                return taskType;
            }

            @Override
            public void execute(TaskContext context) {
                // no-op test handler
            }
        };
    }

    @Test
    void contextCarriesTaskIdentityAndPayload() {
        TaskContext context = new TaskContext(7L, 2, "echo", JsonNodeFactory.instance.objectNode());

        assertThat(context.taskId()).isEqualTo(7L);
        assertThat(context.attempt()).isEqualTo(2);
        assertThat(context.taskType()).isEqualTo("echo");
        assertThat(context.payload().isObject()).isTrue();
    }
}
