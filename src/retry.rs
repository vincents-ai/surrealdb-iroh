//! Retry backoff strategies for connection attempts.
//!
//! This module provides configurable retry logic with exponential backoff
//! for failed connections.

use std::time::Duration;

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Maximum number of retries (0 = infinite)
    pub max_retries: usize,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Jitter factor (0.0-1.0) to add randomness
    pub jitter: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            max_retries: 5,
            backoff_multiplier: 2.0,
            jitter: 0.1,
        }
    }
}

impl RetryConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the initial delay.
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set the maximum delay.
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set the maximum number of retries.
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the backoff multiplier.
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Set the jitter factor.
    pub fn with_jitter(mut self, jitter: f64) -> Self {
        self.jitter = jitter.clamp(0.0, 1.0);
        self
    }
}

/// A retry policy for connection attempts.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Configuration
    config: RetryConfig,
    /// Number of attempts made
    attempts: usize,
}

impl RetryPolicy {
    /// Create a new retry policy.
    pub fn new(config: RetryConfig) -> Self {
        Self {
            config,
            attempts: 0,
        }
    }

    /// Check if we should retry.
    pub fn should_retry(&self) -> bool {
        let max_retries = self.config.max_retries;
        if max_retries == 0 {
            return true;
        }
        self.attempts < max_retries
    }

    /// Get the delay before the next retry.
    pub fn next_delay(&self) -> Option<Duration> {
        if !self.should_retry() {
            return None;
        }

        let base_delay = self.config.initial_delay.as_millis() as f64;
        let multiplier = self.config.backoff_multiplier;
        let delay_ms = base_delay * multiplier.powi(self.attempts as i32);
        let max_ms = self.config.max_delay.as_millis() as f64;
        Some(Duration::from_millis(delay_ms.min(max_ms) as u64))
    }

    /// Record a retry attempt.
    pub fn record_attempt(&mut self) {
        self.attempts += 1;
    }

    /// Record a successful operation.
    pub fn record_success(&mut self) {
        self.attempts = 0;
    }

    /// Get the current number of attempts.
    pub fn attempts(&self) -> usize {
        self.attempts
    }

    /// Reset the policy.
    pub fn reset(&mut self) {
        self.attempts = 0;
    }

    /// Get configuration.
    pub fn config(&self) -> &RetryConfig {
        &self.config
    }
}

/// A retry wrapper for async operations.
pub struct RetryOperation {
    policy: RetryPolicy,
}

impl RetryOperation {
    /// Create a new retry operation.
    pub fn new(config: RetryConfig) -> Self {
        Self {
            policy: RetryPolicy::new(config),
        }
    }

    /// Execute an operation with retries.
    pub async fn execute<F, T, E>(&mut self, mut operation: F) -> Result<T, RetryError<E>>
    where
        F: FnMut() -> Result<T, E>,
    {
        loop {
            match operation() {
                Ok(result) => {
                    self.policy.record_success();
                    return Ok(result);
                }
                Err(e) => {
                    if !self.policy.should_retry() {
                        return Err(RetryError::MaxRetriesExceeded {
                            attempts: self.policy.attempts(),
                            last_error: e,
                        });
                    }

                    let delay = self.policy.next_delay();
                    self.policy.record_attempt();

                    if let Some(delay) = delay {
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
    }

    /// Get the underlying policy.
    pub fn policy(&self) -> &RetryPolicy {
        &self.policy
    }

    /// Reset the policy.
    pub fn reset(&mut self) {
        self.policy.reset();
    }
}

/// Error type for retry operations.
#[derive(Debug)]
pub enum RetryError<E> {
    /// Maximum retries exceeded
    MaxRetriesExceeded {
        /// Number of attempts made
        attempts: usize,
        /// The last error that occurred
        last_error: E,
    },
    /// The operation was cancelled
    Cancelled,
}

impl<E> std::fmt::Display for RetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryError::MaxRetriesExceeded { attempts, .. } => {
                write!(f, "max retries ({}) exceeded", attempts)
            }
            RetryError::Cancelled => {
                write!(f, "operation cancelled")
            }
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for RetryError<E> {}

/// A retry budget for tracking retry costs.
#[derive(Debug)]
pub struct RetryBudget {
    /// Maximum retries per time window
    max_retries: usize,
    /// Time window
    window: Duration,
    /// Current attempt count
    attempts: usize,
}

impl RetryBudget {
    /// Create a new retry budget.
    pub fn new(max_retries: usize, window: Duration) -> Self {
        Self {
            max_retries,
            window,
            attempts: 0,
        }
    }

    /// Check if a retry is allowed.
    pub fn can_retry(&self) -> bool {
        self.attempts < self.max_retries
    }

    /// Record a retry attempt.
    pub fn record_attempt(&mut self) {
        self.attempts += 1;
    }

    /// Get remaining retries.
    pub fn remaining(&self) -> usize {
        self.max_retries.saturating_sub(self.attempts)
    }

    /// Reset the budget.
    pub fn reset(&mut self) {
        self.attempts = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn test_retry_policy_should_retry() {
        let mut policy = RetryPolicy::new(RetryConfig::default());

        assert!(policy.should_retry());

        // Simulate max retries
        for _ in 0..5 {
            policy.record_attempt();
        }

        assert!(!policy.should_retry());
    }

    #[test]
    fn test_retry_policy_infinite() {
        let config = RetryConfig::new().with_max_retries(0);
        let mut policy = RetryPolicy::new(config);

        for _ in 0..100 {
            policy.record_attempt();
            assert!(policy.should_retry());
        }
    }

    #[test]
    fn test_retry_budget() {
        let mut budget = RetryBudget::new(3, Duration::from_secs(1));

        assert!(budget.can_retry());
        budget.record_attempt();
        budget.record_attempt();
        budget.record_attempt();

        assert!(!budget.can_retry());
        assert_eq!(budget.remaining(), 0);
    }
}
