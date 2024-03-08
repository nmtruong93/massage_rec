import os
from functools import lru_cache
from dotenv import load_dotenv


class Config:
    PREFIX = "staging"
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def __init__(self):
        self.S3_DATASET_BUCKET = f'{self.PREFIX}-massage-dataset'
        self.DATASET_GROUP_NAME = f'{self.PREFIX}-massage-dataset-group'
        self.PERSONALIZE_ROLE_ARN = f'arn:aws:iam::123456789012:role/PersonalizeRole'


class StagingConfig(Config):
    PREFIX = "staging"


class ProductionConfig(Config):
    PREFIX = "prod"


@lru_cache()
def get_settings():
    config_cls_dict = {"staging": StagingConfig, "prod": ProductionConfig}
    env_dict = {"staging": ".env.staging", "prod": ".env.prod"}

    config_name = os.getenv("ENV", "staging")
    config_cls = config_cls_dict.get(config_name)
    env_path = os.path.join(Config.BASE_DIR, env_dict.get(config_name))
    print("Load env from ", env_path)
    load_dotenv(env_path)

    return config_cls()


settings = get_settings()
