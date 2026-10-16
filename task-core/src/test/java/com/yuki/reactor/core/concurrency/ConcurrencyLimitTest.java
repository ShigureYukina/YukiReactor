package com.yuki.reactor.core.concurrency;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The limit is the only place where the adaptive controller and the worker meet, so its clamping
 * rules have to hold without any Spring context and without Docker.
 */
class ConcurrencyLimitTest {

    @Test
    void fixedLimitNeverMoves() {
        ConcurrencyLimit limit = ConcurrencyLimit.fixed(4);

        assertThat(limit.min()).isEqualTo(4);
        assertThat(limit.max()).isEqualTo(4);
        assertThat(limit.current()).isEqualTo(4);
        assertThat(limit.apply(10)).isEqualTo(4);
        assertThat(limit.apply(1)).isEqualTo(4);
        assertThat(limit.current()).isEqualTo(4);
    }

    @Test
    void applyClampsToTheConfiguredBoundsAndReturnsTheEffectiveValue() {
        ConcurrencyLimit limit = new ConcurrencyLimit(2, 8, 5);

        assertThat(limit.current()).isEqualTo(5);
        assertThat(limit.apply(9)).isEqualTo(8);
        assertThat(limit.current()).isEqualTo(8);
        assertThat(limit.apply(1)).isEqualTo(2);
        assertThat(limit.current()).isEqualTo(2);
        assertThat(limit.apply(6)).isEqualTo(6);
        assertThat(limit.current()).isEqualTo(6);
    }

    @Test
    void clampDoesNotChangeTheCurrentValue() {
        ConcurrencyLimit limit = new ConcurrencyLimit(2, 8, 5);

        assertThat(limit.clamp(100)).isEqualTo(8);
        assertThat(limit.clamp(-3)).isEqualTo(2);
        assertThat(limit.clamp(7)).isEqualTo(7);
        assertThat(limit.current()).isEqualTo(5);
    }

    @Test
    void initialValueIsClampedIntoTheBounds() {
        assertThat(new ConcurrencyLimit(2, 8, 100).current()).isEqualTo(8);
        assertThat(new ConcurrencyLimit(2, 8, 0).current()).isEqualTo(2);
        assertThat(new ConcurrencyLimit(2, 8, 2).current()).isEqualTo(2);
    }

    @Test
    void rejectsBoundsThatCouldNeverWork() {
        assertThatThrownBy(() -> new ConcurrencyLimit(0, 8, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("min");
        assertThatThrownBy(() -> new ConcurrencyLimit(8, 2, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max");
        assertThatThrownBy(() -> ConcurrencyLimit.fixed(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
