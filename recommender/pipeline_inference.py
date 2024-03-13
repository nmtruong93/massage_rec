from helpers.connection import connect_to_personalize_runtime


class Inference:
    def __init__(self, profile_name=None):
        self.profile_name = profile_name
        self.personalize_runtime_client = connect_to_personalize_runtime(profile_name=profile_name)

    def get_recommendations(self, campaign_arn, user_id, context, num_results=5, return_item_metadata=True):
        """
        Get recommendations
        Example:
            campaign_arn = 'arn:aws:personalize:us-west-2:123456789012:campaign/staging-massage-campaign'
            user_id = 'da5cc281-7dae-4ef6-9d46-580102ec0784'
            context = {
                'SERVICE_LENGTH': "60.0",
                'MASSAGE_NAME': 'The NOW 50',
                'CENTER_NAME': 'Roswell',
                'AGE': "30",
                'GENDER': 'Female',
                'ZIPCODE': "null",
                'BASE_CENTER': 'Roswell'
            }
            num_results = 5
            return_item_metadata = True
            inference = Inference()
            inference.get_recommendations(campaign_arn, user_id, context, num_results, return_item_metadata)

        :param campaign_arn: str, the campaign ARN
        :param user_id: str, the user ID
        :param context: dict, the context
        :param num_results: int, the number of results
        :param return_item_metadata: bool, whether to return item metadata. Default: True
        :return: list, the recommendations
        """
        params = {
            "campaignArn": campaign_arn,
            "userId": user_id,
            "numResults": num_results,
            "context": context
        }
        if return_item_metadata:
            params['metadataColumns'] = {
                "ITEMS": ["ITEM_NAME"]
            }

        response = self.personalize_runtime_client.get_recommendations(**params)
        return response['itemList']


if __name__ == '__main__':
    inference = Inference(profile_name='nmtruong')
    item_list = inference.get_recommendations(
        'arn:aws:personalize:us-west-2:123456789012:campaign/staging-massage-campaign',
        'da5cc281-7dae-4ef6-9d46-580102ec0784',
        {
            'SERVICE_LENGTH': "60.0",
            'MASSAGE_NAME': 'The NOW 50',
            'CENTER_NAME': 'Roswell',
            'AGE': "30",
            'GENDER': 'Female',
            'ZIPCODE': "null",
            'BASE_CENTER': 'Roswell'
        }
    )
    print(item_list)
