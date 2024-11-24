import os


class Config:
    def __init__(self, additional_vars: list = []):
        required_vars = ["task_name", "db_host", "db_user", "db_password", "db_name"]
        self.required_vars = required_vars + additional_vars
        self._validate_required_vars()

    def _validate_required_vars(self):
        missing_vars = [var for var in self.required_vars if not os.getenv(var)]
        if missing_vars:
            raise Exception(f"missing required env - {', '.join(missing_vars)}")

    @property
    def configs(self):
        additional_cfg = {}
        task_name = os.getenv("task_name")

        default_cfg = {
            "task_name": task_name,
            "database": {
                "engine": os.getenv("db_engine", "postgres"),
                "host": os.getenv("db_host"),
                "port": os.getenv("db_port", 5432),
                "user": os.getenv("db_user"),
                "password": os.getenv("db_password"),
                "db_name": os.getenv("db_name"),
            },
        }

        if task_name == "api_retrieval":
            additional_cfg["api"] = {
                "url": os.getenv("api_url"),
                "key": os.getenv("api_key", ""),
            }

        return {**default_cfg, **additional_cfg}
