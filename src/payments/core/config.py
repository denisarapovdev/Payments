from functools import lru_cache

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"

    # --- infrastructure ---
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    db_pool_size: int = 10
    rabbit_url: str = "amqp://guest:guest@localhost:5672/"

    # --- API ---
    api_key: SecretStr = SecretStr("change-me")

    # --- outbox relay ---
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    # --- consumer ---
    consumer_prefetch: int = Field(default=10, gt=0)
    max_attempts: int = Field(default=3, ge=1, description="Total delivery attempts, incl. first")
    retry_base_delay: float = Field(default=2.0, gt=0, description="Seconds before 1st retry")
    retry_multiplier: float = Field(default=2.0, ge=1)

    # --- gateway emulator ---
    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    # --- webhooks ---
    webhook_timeout: float = Field(default=5.0, gt=0)
    webhook_secret: SecretStr = SecretStr("change-me-too")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def retry_delays_ms(self) -> list[int]:
        """Delay before each retry: base * multiplier^n, one entry per retry (max_attempts - 1)."""
        return [
            int(self.retry_base_delay * self.retry_multiplier**n * 1000)
            for n in range(self.max_attempts - 1)
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()
