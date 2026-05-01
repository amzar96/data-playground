from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    s3_endpoint: str
    s3_key: str
    s3_secret: str
    s3_bucket: str
    s3_region: str
    catalog_db: str

    def catalog_properties(self) -> dict[str, str]:
        return {
            "uri": self.catalog_db,
            "warehouse": f"s3://{self.s3_bucket}",
            "io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3.endpoint": self.s3_endpoint,
            "s3.access-key-id": self.s3_key,
            "s3.secret-access-key": self.s3_secret,
            "s3.region": self.s3_region,
            "s3.path-style-access": "true",
        }


settings = Settings()
