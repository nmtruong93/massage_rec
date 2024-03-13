import os
from recommender.data_loader import DataLoader
from recommender.dataset_builder import DatasetBuilder
from recommender.personalization import Personalization

from config.config import settings
from config.log_config import logger
from helpers.aws_data_ops import upload_file_to_s3
from helpers.connection import connect_to_s3_client


class TrainPipeline:
    """
    Full pipeline to train and release ARN for the recommendation model
    """
    def __init__(self, data_path, profile_name=None):
        self.data_path = data_path
        self.profile_name = profile_name
        self.deploy_env = os.getenv('DEPLOY_ENV', 'staging').lower()
        self.s3_client = connect_to_s3_client(profile_name=profile_name)

    def process_data(self):
        """
        Load raw data and process it

        :return: pd.DataFrame, the processed data
        """
        data_loader = DataLoader(self.data_path)
        data = data_loader.load_data()
        merged_df = data_loader.merge_massages_enhancements(data)
        processed_df = data_loader.process_data_types(merged_df)
        return processed_df

    def build_data_for_personalize(self, process_data):
        """
        Prepare user, item and interaction data for personalize and upload to S3 bucket

        :return: None
        """
        data_builder = DatasetBuilder(data=process_data)
        interaction_df = data_builder.build_interaction_dataset()
        user_df = data_builder.build_user_dataset()
        item_df = data_builder.build_item_dataset()

        os.makedirs(os.path.join(settings.BASE_DIR, 'data'), exist_ok=True)

        interaction_df.to_csv(os.path.join(settings.BASE_DIR, 'data/interaction.csv'), index=False)
        user_df.to_csv(os.path.join(settings.BASE_DIR, 'data/user.csv'), index=False)
        item_df.to_csv(os.path.join(settings.BASE_DIR, 'data/item.csv'), index=False)

        # TODO: Check if S3 bucket exists

        upload_file_to_s3(
            self.s3_client,
            os.path.join(settings.BASE_DIR, 'data/interaction.csv'),
            settings.S3_DATASET_BUCKET,
            'interaction.csv'
        )
        upload_file_to_s3(
            self.s3_client,
            os.path.join(settings.BASE_DIR, 'data/user.csv'),
            settings.S3_DATASET_BUCKET,
            'user.csv'
        )
        upload_file_to_s3(
            self.s3_client,
            os.path.join(settings.BASE_DIR, 'data/item.csv'),
            settings.S3_DATASET_BUCKET,
            'item.csv'
        )
        logger.info("Data uploaded to s3")

    def train_recommendation(self, import_mode='INCREMENTAL'):
        """
        Train the recommendation model

        :param import_mode: str, str, the import data mode, 'FULL'|'INCREMENTAL'. Default: 'FULL'
        :return: str, the ARN of the campaign, this is endpoint for the recommendation model
        """

        personalize = Personalization(profile_name=self.profile_name)
        dataset_group_arn = personalize.create_dataset_group(name=f'{self.deploy_env}-massage-dataset-group')

        # Create datasets
        interaction_dataset_arn = personalize.create_interaction_dataset(
            schema_name=f'{self.deploy_env}-massage-interactions-schema',
            dataset_group_arn=dataset_group_arn,
            name=f'{self.deploy_env}-massage-interactions'
        )
        user_dataset_arn = personalize.create_user_dataset(
            schema_name=f'{self.deploy_env}-massage-users-schema',
            dataset_group_arn=dataset_group_arn,
            name=f'{self.deploy_env}-massage-users'
        )
        item_dataset_arn = personalize.create_item_dataset(
            schema_name=f'{self.deploy_env}-massage-items-schema',
            dataset_group_arn=dataset_group_arn,
            name=f'{self.deploy_env}-massage-items'
        )

        # Import the data
        s3_data_path = f"s3://{settings.S3_DATASET_BUCKET}/interaction.csv"
        personalize.import_interactions_data(interaction_dataset_arn, s3_data_path, import_mode=import_mode)

        s3_data_path = f"s3://{settings.S3_DATASET_BUCKET}/user.csv"
        personalize.import_users_data(user_dataset_arn, s3_data_path, import_mode=import_mode)

        s3_data_path = f"s3://{settings.S3_DATASET_BUCKET}/item.csv"
        personalize.import_items_data(item_dataset_arn, s3_data_path, import_mode=import_mode)

        # Create a solution, aka train the model
        solution_version_arn = personalize.create_solution(
            name=f'{self.deploy_env}-massage-solution',
            dataset_group_arn=dataset_group_arn
        )

        # Get solution metrics, aka ranking metrics
        solution_metrics = personalize.get_solution_metrics(solution_version_arn)
        logger.info(solution_metrics)

        # Create a campaign
        campaign_arn = personalize.create_campaign(
            name=f'{self.deploy_env}-massage-campaign',
            solution_version_arn=solution_version_arn
        )
        logger.info(campaign_arn)
        return campaign_arn

    def run(self, import_mode='INCREMENTAL'):
        processed_data = self.process_data()
        self.build_data_for_personalize(processed_data)
        campaign_arn = self.train_recommendation(import_mode=import_mode)
        return campaign_arn


if __name__ == '__main__':
    data_path = os.path.join(settings.BASE_DIR, 'data', 'full_2024-02-22_04-55-21.csv')
    train_pipeline = TrainPipeline(data_path, profile_name='nmtruong')
    train_pipeline.run(import_mode='FULL')
