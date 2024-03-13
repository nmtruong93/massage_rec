import pandas as pd
from config.config import settings
from config.log_config import logger


class DatasetBuilder:
    """
    Build the dataset for the recommendation system, including interaction, user and item datasets
    The dataset will be uploaded to S3 bucket for Personalize to use.
    The format of the dataset is csv
    """

    def __init__(self, data=None, data_path=None):
        if data is None:
            self.data = self.load_data(data_path)
        else:
            self.data = data

    @staticmethod
    def load_data(data_path):
        """
        Load the data

        :param data_path: str, the data path
        :return: pd.DataFrame, the data
        """
        logger.info("Loading data...")

        return pd.read_parquet(data_path)

    def build_interaction_dataset(self):
        """
        Build the interaction dataset for the recommendation system

        :return: pd.DataFrame, the interaction dataset
        """
        logger.info("Building interaction dataset...")

        # Create the interaction dataset
        interaction_df = self.data[['user_id', 'item_id', 'timestamp', 'service_length', 'massage_name', 'center_name']]
        interaction_df['event_type'] = 'purchase'
        # interaction_df = interaction_df.drop_duplicates(subset=['user_id', 'item_id', 'timestamp'])
        interaction_df.columns = [col.upper() for col in interaction_df.columns]

        # Convert timestamp to unix timestamp
        interaction_df['TIMESTAMP'] = interaction_df['TIMESTAMP'].astype(int) // 10 ** 9

        logger.info(f"Shape of interaction dataset: {interaction_df.shape}")
        return interaction_df

    def build_user_dataset(self):
        """
        Build the user dataset for the recommendation system

        :return: pd.DataFrame, the user dataset
        """
        logger.info("Building user dataset...")

        # Create the user dataset
        user_df = self.data[['user_id', 'age', 'gender', 'zipcode', 'base_center']]
        user_df = user_df.drop_duplicates(subset=['user_id'])
        user_df.columns = [col.upper() for col in user_df.columns]
        return user_df

    def build_item_dataset(self):
        """
        Build the item dataset for the recommendation system

        :return: pd.DataFrame, the item dataset
        """
        logger.info("Building item dataset...")

        # Create the item dataset
        item_df = self.data[['item_id', 'item_name']]
        item_df = item_df.drop_duplicates(subset=['item_id'])
        item_df.columns = [col.upper() for col in item_df.columns]
        return item_df
