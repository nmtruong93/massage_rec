import os

from config.config import settings
from config.log_config import logger
import pandas as pd


class DataLoader:
    """
    The recommender will recommend the enhancement based on the massage that the customer has chosen.
    The customers go to the massages and can choose to add >= 1 enhancements to their massages.

    https://thenowmassage.com/
    """

    def __init__(self, data_path):
        self.data_path = data_path

    def load_data(self):
        """
        The customers go to the massages and can choose to add >= 1 enhancements to their massages.

        :return: pd.DataFrame, the data
        """
        logger.info("Loading data...")

        df = pd.read_csv(self.data_path)
        df.columns = [i.lower().replace(' ', '_') for i in df.columns]

        # Drop unnecessary columns
        drop_cols = ['service_name', 'center_zip']
        df.drop(columns=drop_cols, inplace=True)

        # Only calculate the recommendation for the massages and enhancements
        df = df[df.service_parent_category.isin(['Massages', 'Enhancement'])]

        # 1 invoice_id has only 1 massages, but can have multiple enhancements
        # --> remove duplicates for massages with the same invoice_id

        index_li = df[['invoice_id', 'item_name']].drop_duplicates().index

        return df.loc[index_li]

    @staticmethod
    def merge_massages_enhancements(data):
        """
        Create connection between massage and enhancement by split message and enhancement into 2 different columns

        :return:
        """
        logger.info("Merging massages and enhancements...")

        massage_cols = ['user_id', 'guest_dob', 'guest_zipcode', 'guest_gender', 'guest_base_center', 'invoice_id',
                        'service_length', 'service_category', 'item_code', 'center_name']
        enhancement_cols = ['invoice_id', 'item_code', 'invoice_closed_date']
        massage_df = data[data.service_parent_category == 'Massages'][massage_cols]
        enhancement_df = data[data.service_parent_category == 'Enhancement'][enhancement_cols]

        massage_df.rename(
            columns={
                'service_category': 'massage_category',
                'item_code': 'massage_item_code'
            },
            inplace=True
        )
        enhancement_df.rename(
            columns={
                'item_code': 'enhancement_item_code'
            },
            inplace=True
        )

        # Merge the massage and enhancement data
        merged_df = massage_df.merge(enhancement_df, on='invoice_id', how='left')
        merged_df = merged_df[~merged_df.enhancement_item_code.isnull()].reset_index(drop=True)

        return merged_df

    @staticmethod
    def process_data_types(data, days_in_year=365.25):
        """
        Process the data types of the data

        :param data: pd.DataFrame, the data to process
        :param days_in_year: float, the number of days in a year
        :return:
        """
        logger.info("Processing data types...")

        # Convert 9/20/1978 12:00:00 AM to datetime
        data['guest_dob'] = pd.to_datetime(data['guest_dob'], format='%m/%d/%Y %I:%M:%S %p')
        data['invoice_closed_date'] = pd.to_datetime(data['invoice_closed_date'])

        # Calculate the age of the guest
        data['guest_age'] = (pd.to_datetime('today') - data['guest_dob']).dt.days // days_in_year
        data.drop(columns='guest_dob', inplace=True)

        cat_cols = [
            'guest_zipcode', 'guest_gender', 'guest_base_center', 'service_length',
            'massage_category', 'massage_item_code', 'center_name'
        ]
        for col in cat_cols:
            data[col] = data[col].astype('category')

        logger.info(f"Data rows {data.shape[0]} "
                    f"| Unique users {data.user_id.nunique()} "
                    f"| unique items {data.enhancement_item_code.nunique()} "
                    f"| unique invoices {data.invoice_id.nunique()}")

        return data


if __name__ == '__main__':
    data_loader = DataLoader(os.path.join(settings.BASE_DIR, 'data', '10000_samples.csv'))
    data = data_loader.load_data()
    merged_df = data_loader.merge_massages_enhancements(data)
    processed_df = data_loader.process_data_types(merged_df)
