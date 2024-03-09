import os
import time
import json
from botocore.exceptions import ClientError
from config.config import settings
from config.log_config import logger
from helpers.connection import (connect_to_personalize, connect_to_iam_resource, connect_to_personalize_runtime,
                                connect_to_s3_client)


class Personalize:
    def __init__(self, profile_name=None):
        self.s3_client = connect_to_s3_client(profile_name=profile_name)
        self.personalize_client = connect_to_personalize(profile_name=profile_name)
        self.iam_resource = connect_to_iam_resource(profile_name=profile_name)
        self.personalize_runtime_client = connect_to_personalize_runtime(profile_name=profile_name)

        self.allow_personalize_access_to_s3(settings.S3_DATASET_BUCKET)
        role = self.create_iam_role(settings.PERSONALIZE_ROLE_NAME)
        self.role_arn = role.arn

    def allow_personalize_access_to_s3(self, bucket_name):
        """
        Allow personalize access to s3 bucket

        :param bucket_name: str, the s3 bucket name
        :return:
        """
        policy = {
            "Version": "2012-10-17",
            "Id": "PersonalizeS3BucketAccessPolicy",
            "Statement": [
                {
                    "Sid": "PersonalizeS3BucketAccessPolicy",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "personalize.amazonaws.com"
                    },
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:PutObject",
                    ],
                    "Resource": [
                        "arn:aws:s3:::{}".format(bucket_name),
                        "arn:aws:s3:::{}/*".format(bucket_name)
                    ]
                }
            ]
        }

        self.s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
        logger.info(f"Allowed personalize access to s3 bucket {bucket_name}")

    def get_iam_role(self, iam_role_name):
        """
        Get an AWS Identity and Access Management (IAM) role.

        :param iam_role_name: The name of the role to retrieve.
        :return: The IAM role.
        """
        role = None
        try:
            temp_role = self.iam_resource.Role(iam_role_name)
            temp_role.load()
            role = temp_role
            logger.info("Got IAM role %s", role.name)
        except ClientError as err:
            if err.response["Error"]["Code"] == "NoSuchEntity":
                logger.info("IAM role %s does not exist.", iam_role_name)
            else:
                logger.error(
                    "Couldn't get IAM role %s. Here's why: %s: %s",
                    iam_role_name,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
                raise
        return role

    def create_iam_role(self, iam_role_name, policy_arn=None):
        """
         Creates an IAM role that grants the aws Personalize permissions. If a
         role with the specified name already exists, it is used for the demo.

         :param iam_role_name: The name of the role to create.
         :param policy_arn: str | list, The Amazon Resource Name (ARN) of a policy to attach to the role.
                             Default: AWSLambdaBasicExecutionRole
         :return: The role and a value that indicates whether the role is newly created.
         """
        role = self.get_iam_role(iam_role_name)
        if role is not None:
            return role

        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "personalize.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        if not policy_arn:
            policy_arn = [
                "arn:aws:iam::aws:policy/AmazonS3FullAccess",
                "arn:aws:iam::aws:policy/service-role/AmazonPersonalizeFullAccess"
                ]

        if isinstance(policy_arn, str):
            policy_arn = [policy_arn]

        try:
            role = self.iam_resource.create_role(
                RoleName=iam_role_name,
                AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            )
            logger.info(f"Created role {role.name}")

            for arn in policy_arn:
                role.attach_policy(PolicyArn=arn)
                logger.info(f"Attached {arn} policy to role {role.name}")

        except ClientError as error:
            if error.response["Error"]["Code"] == "EntityAlreadyExists":
                role = self.iam_resource.Role(iam_role_name)
                logger.warning("The role %s already exists. Using it.", iam_role_name)
            else:
                logger.exception(
                    "Couldn't create role %s or attach policy %s.",
                    iam_role_name,
                    policy_arn,
                )
        logger.info("Giving AWS time to create resources...")
        time.sleep(10)
        return role

    def create_dataset_group(self, name):
        """
        Create a dataset group

        :param name: str, the name of the dataset group
        :return: str, the dataset group ARN
        """
        logger.info(f"Creating dataset group {name}...")

        # Check if the dataset group already exists
        dataset_groups = self.personalize_client.list_dataset_groups()
        for dataset_group in dataset_groups['datasetGroups']:
            if dataset_group['name'] == name:
                return dataset_group['datasetGroupArn']

        response = self.personalize_client.create_dataset_group(name=name)
        dataset_group_arn = response['datasetGroupArn']

        # Wait for the dataset group to be created
        max_time = time.time() + 3 * 60 * 60  # 3 hours
        while time.time() < max_time:
            describe_dataset_group_response = self.personalize_client.describe_dataset_group(
                datasetGroupArn=dataset_group_arn
            )
            status = describe_dataset_group_response["datasetGroup"]["status"]
            logger.info(f"DatasetGroup: {status}")

            if status == "ACTIVE" or status == "CREATE FAILED":
                break

            time.sleep(10)

        return response['datasetGroupArn']

    def create_interaction_dataset(self, schema_name, dataset_group_arn, name):
        """
        Create an interaction dataset

        :param schema_name: str, the name of the schema
        :param dataset_group_arn: str, the dataset group ARN
        :param name: str, the name of the dataset
        :return: str, the dataset ARN
        """
        logger.info(f"Creating interaction schema {schema_name}...")

        # Check if the dataset already exists
        datasets = self.personalize_client.list_datasets(datasetGroupArn=dataset_group_arn)
        for dataset in datasets['datasets']:
            if dataset['name'] == name:
                return dataset['datasetArn']

        # USER_ID,ITEM_ID,TIMESTAMP,SERVICE_LENGTH,MASSAGE_NAME,CENTER_NAME,EVENT_TYPE
        schema = {
            "type": "record",
            "name": "Interactions",
            "namespace": "com.amazonaws.personalize.schema",
            "fields": [
                {
                    "name": "USER_ID",
                    "type": "string"
                },
                {
                    "name": "ITEM_ID",
                    "type": "string"
                },
                {
                    "name": "TIMESTAMP",
                    "type": "long"
                },
                {
                    "name": "SERVICE_LENGTH",
                    "type": ["float", "null"],
                    "categorical": True
                },
                {
                    "name": "MASSAGE_NAME",
                    "type": ["string", "null"],
                    "categorical": True
                },
                {
                    "name": "CENTER_NAME",
                    "type": ["string", "null"],
                    "categorical": True
                },
                {
                    "name": "EVENT_TYPE",
                    "type": "string"
                }
            ],
            "version": "1.0"
        }
        schema_response = self.personalize_client.create_schema(
            name=schema_name,
            schema=json.dumps(schema)
        )
        schema_arn = schema_response['schemaArn']

        logger.info(f"Creating interaction dataset {name}...")
        dataset_response = self.personalize_client.create_dataset(
            datasetType='INTERACTIONS',
            datasetGroupArn=dataset_group_arn,
            schemaArn=schema_arn,
            name=name,
        )

        self.wait_create_dataset(dataset_response)
        return dataset_response['datasetArn']

    def create_user_dataset(self, schema_name, dataset_group_arn, name):
        """
        Create a user dataset

        :param schema_name: str, the name of the schema
        :param dataset_group_arn: str, the dataset group ARN
        :param name: str, the name of the dataset
        :return: str, the dataset ARN
        """
        logger.info(f"Creating user schema {schema_name}...")
        # Check if the dataset already exists
        datasets = self.personalize_client.list_datasets(datasetGroupArn=dataset_group_arn)
        for dataset in datasets['datasets']:
            if dataset['name'] == name:
                return dataset['datasetArn']

        # USER_ID, AGE, GENDER, ZIPCODE, BASE_CENTER, CENTER_NAME
        schema = {
            "type": "record",
            "name": "Users",
            "namespace": "com.amazonaws.personalize.schema",
            "fields": [
                {
                    "name": "USER_ID",
                    "type": "string"
                },
                {
                    "name": "AGE",
                    "type": ["int", "null"]
                },
                {
                    "name": "GENDER",
                    "type": "string",
                    "categorical": True
                },
                {
                    "name": "ZIPCODE",
                    "type": ["string", "null"],
                    "categorical": True
                },
                {
                    "name": "BASE_CENTER",
                    "type": ["string", "null"],
                    "categorical": True
                }
            ],
            "version": "1.0"
        }
        schema_response = self.personalize_client.create_schema(
            name=schema_name,
            schema=json.dumps(schema)
        )
        schema_arn = schema_response['schemaArn']

        logger.info(f"Creating user dataset {name}...")
        dataset_response = self.personalize_client.create_dataset(
            datasetType='USERS',
            datasetGroupArn=dataset_group_arn,
            schemaArn=schema_arn,
            name=name,
        )
        self.wait_create_dataset(dataset_response)
        return dataset_response['datasetArn']

    def create_item_dataset(self, schema_name, dataset_group_arn, name):
        """
        Create an item dataset

        :param schema_name: str, the name of the schema
        :param dataset_group_arn: str, the dataset group ARN
        :param name: str, the name of the dataset
        :return: str, the dataset ARN
        """
        logger.info(f"Creating item schema {schema_name}...")
        # Check if the dataset already exists
        datasets = self.personalize_client.list_datasets(datasetGroupArn=dataset_group_arn)
        for dataset in datasets['datasets']:
            if dataset['name'] == name:
                return dataset['datasetArn']

        # ITEM_ID, ITEM_NAME
        schema = {
            "type": "record",
            "name": "Items",
            "namespace": "com.amazonaws.personalize.schema",
            "fields": [
                {
                    "name": "ITEM_ID",
                    "type": "string"
                },
                {
                    "name": "ITEM_NAME",
                    "type": ["string", "null"],
                    "textual": True,
                }
            ],
            "version": "1.0"
        }
        schema_response = self.personalize_client.create_schema(
            name=schema_name,
            schema=json.dumps(schema)
        )
        schema_arn = schema_response['schemaArn']

        logger.info(f"Creating item dataset {name}...")
        dataset_response = self.personalize_client.create_dataset(
            datasetType='ITEMS',
            datasetGroupArn=dataset_group_arn,
            schemaArn=schema_arn,
            name=name,
        )

        self.wait_create_dataset(dataset_response)
        return dataset_response['datasetArn']

    def wait_create_dataset(self, dataset_response):
        """
        Wait for the dataset to be created

        :param dataset_response: dict, the dataset response
        :return:
        """
        # Wait for the dataset to be created
        max_time = time.time() + 3 * 60 * 60
        while time.time() < max_time:
            describe_dataset_response = self.personalize_client.describe_dataset(
                datasetArn=dataset_response['datasetArn']
            )
            status = describe_dataset_response["dataset"]["status"]
            logger.info(f"Dataset: {status}")

            if status == "ACTIVE" or status == "CREATE FAILED":
                break

            time.sleep(30)

    def import_interactions_data(self, dataset_arn, s3_data_path, import_mode='FULL'):
        """
        Import interactions data to the dataset

        :param dataset_arn: str, the dataset ARN
        :param s3_data_path: str, the path to the data
        :param import_mode: str, the import mode, 'FULL'|'INCREMENTAL'. Default: 'FULL'
        :return: str, the job ARN
        """
        logger.info(f"Importing interactions data to {dataset_arn}...")

        response = self.personalize_client.create_dataset_import_job(
            jobName=f"massage-interactions-import-{int(time.time())}",
            datasetArn=dataset_arn,
            dataSource={
                "dataLocation": s3_data_path
            },
            roleArn=self.role_arn,
            importMode=import_mode
        )

        self.wait_import_dataset(response)

        return response['datasetImportJobArn']

    def import_users_data(self, dataset_arn, s3_data_path, import_mode='FULL'):
        """
        Import users data to the dataset

        :param dataset_arn: str, the dataset ARN
        :param s3_data_path: str, the path to the data
        :param import_mode: str, the import mode, 'FULL'|'INCREMENTAL'. Default: 'FULL'
        :return: str, the job ARN
        """
        logger.info(f"Importing users data to {dataset_arn}...")

        response = self.personalize_client.create_dataset_import_job(
            jobName=f"massage-users-import-{int(time.time())}",
            datasetArn=dataset_arn,
            dataSource={
                "dataLocation": s3_data_path
            },
            roleArn=self.role_arn,
            importMode=import_mode
        )

        self.wait_import_dataset(response)

        return response['datasetImportJobArn']

    def import_items_data(self, dataset_arn, s3_data_path, import_mode='FULL'):
        """
        Import items data to the dataset

        :param dataset_arn: str, the dataset ARN
        :param s3_data_path: str, the path to the data
        :param import_mode: str, the import mode, 'FULL'|'INCREMENTAL'. Default: 'FULL'
        :return: str, the job ARN
        """
        logger.info(f"Importing items data to {dataset_arn}...")

        response = self.personalize_client.create_dataset_import_job(
            jobName=f"massage-items-import-{int(time.time())}",
            datasetArn=dataset_arn,
            dataSource={
                "dataLocation": s3_data_path
            },
            roleArn=self.role_arn,
            importMode=import_mode
        )

        self.wait_import_dataset(response)

        return response['datasetImportJobArn']

    def wait_import_dataset(self, response):
        # Wait for the dataset import job to be created
        max_time = time.time() + 3 * 60 * 60
        while time.time() < max_time:
            describe_dataset_import_job_response = self.personalize_client.describe_dataset_import_job(
                datasetImportJobArn=response['datasetImportJobArn']
            )

            dataset_import_job = describe_dataset_import_job_response["datasetImportJob"]
            if "latestDatasetImportJobRun" not in dataset_import_job:
                status = dataset_import_job["status"]
                logger.info(f"DatasetImportJob: {status}")
            else:
                status = dataset_import_job["latestDatasetImportJobRun"]["status"]
                logger.info(f"LatestDatasetImportJobRun: {status}")

            if status == "ACTIVE" or status == "CREATE FAILED":
                break

            time.sleep(30)

    def create_solution(self, name, dataset_group_arn, recipe_arn=None):
        """
        Create a solution

        :param name: str, the name of the solution
        :param dataset_group_arn: str, the dataset group ARN
        :param recipe_arn: str, the recipe ARN
        :return: str, the solution ARN
        """
        logger.info("List recipes...")
        recipes = self.personalize_client.list_recipes()
        for recipe in recipes['recipes']:
            logger.info(f"Recipe: {recipe['recipeArn']}")

        if not recipe_arn:
            recipe_arn = "arn:aws:personalize:::recipe/aws-user-personalization"

        logger.info(f"Creating solution {name}...")
        solution_response = self.personalize_client.create_solution(
            name=name,
            datasetGroupArn=dataset_group_arn,
            recipeArn=recipe_arn,
            performHPO=True
        )

        solution_version_response = self.personalize_client.create_solution_version(
            solutionArn=solution_response['solutionArn']
        )

        # Wait for the solution version to be created
        max_time = time.time() + 3 * 60 * 60  # 3 hours
        while time.time() < max_time:
            describe_solution_version_response = self.personalize_client.describe_solution_version(
                solutionVersionArn=solution_version_response["solutionVersionArn"]
            )
            status = describe_solution_version_response["solutionVersion"]["status"]
            logger.info(f"SolutionVersion: {status}")

            if status == "ACTIVE" or status == "CREATE FAILED":
                break

            time.sleep(30)

        return solution_response['solutionArn'], solution_version_response['solutionVersionArn']

    def get_solution_metrics(self, solution_version_arn):
        """
        Get solution metrics

        :param solution_version_arn: str, the solution version ARN
        :return: dict, the solution metrics
        """
        response = self.personalize_client.get_solution_metrics(
            solutionVersionArn=solution_version_arn
        )
        return response['metrics']

    def create_campaign(self, name, solution_version_arn, min_provisioned_tps=2):
        """
        Create a campaign

        :param name: str, the name of the campaign
        :param solution_version_arn: str, the solution version ARN
        :param min_provisioned_tps: int, the minimum provisioned transactions per second
        :return: str, the campaign ARN
        """
        logger.info(f"Creating campaign {name}...")
        response = self.personalize_client.create_campaign(
            name=name,
            solutionVersionArn=solution_version_arn,
            minProvisionedTPS=min_provisioned_tps
        )

        # Wait for the campaign to be created
        max_time = time.time() + 3 * 60 * 60  # 3 hours
        while time.time() < max_time:
            describe_campaign_response = self.personalize_client.describe_campaign(
                campaignArn=response['campaignArn']
            )
            status = describe_campaign_response["campaign"]["status"]
            logger.info(f"Campaign: {status}")

            if status == "ACTIVE" or status == "CREATE FAILED":
                break

            time.sleep(30)
        return response['campaignArn']

    def get_recommendations(self, campaign_arn, user_id, context, num_results=5):
        """
        Get recommendations

        :param campaign_arn: str, the campaign ARN
        :param user_id: str, the user ID
        :param context: dict, the context
        :param num_results: int, the number of results
        :return: list, the recommendations
        """
        response = self.personalize_runtime_client.get_recommendations(
            campaignArn=campaign_arn,
            userId=user_id,
            numResults=num_results,
            context=context
        )
        return response['itemList']


if __name__ == '__main__':
    deploy_env = os.getenv('DEPLOY_ENV', 'staging').lower()
    personalize = Personalize(profile_name='nmtruong')
    dataset_group_arn = personalize.create_dataset_group(name=f'{deploy_env}-massage-dataset-group')
    interaction_dataset_arn = personalize.create_interaction_dataset(
        schema_name=f'{deploy_env}-massage-interactions-schema',
        dataset_group_arn=dataset_group_arn,
        name=f'{deploy_env}-massage-interactions'
    )
    user_dataset_arn = personalize.create_user_dataset(
        schema_name=f'{deploy_env}-massage-users-schema',
        dataset_group_arn=dataset_group_arn,
        name=f'{deploy_env}-massage-users'
    )
    item_dataset_arn = personalize.create_item_dataset(
        schema_name=f'{deploy_env}-massage-items-schema',
        dataset_group_arn=dataset_group_arn,
        name=f'{deploy_env}-massage-items'
    )

    # Import the data
    s3_data_path = f"s3://{settings.S3_DATASET_BUCKET}/interaction.csv"
    personalize.import_interactions_data(interaction_dataset_arn, s3_data_path)

    s3_data_path = f"s3://{settings.S3_DATASET_BUCKET}/user.csv"
    personalize.import_users_data(user_dataset_arn, s3_data_path)

    s3_data_path = f"s3://{settings.S3_DATASET_BUCKET}/item.csv"
    personalize.import_items_data(item_dataset_arn, s3_data_path)

    # Create a solution
    solution_arn, solution_version_arn = personalize.create_solution(
        name=f'{deploy_env}-massage-solution',
        dataset_group_arn=dataset_group_arn
    )

    # Get solution metrics
    solution_metrics = personalize.get_solution_metrics(solution_version_arn)
    logger.info(solution_metrics)

    # Create a campaign
    campaign_arn = personalize.create_campaign(
        name=f'{deploy_env}-massage-campaign',
        solution_version_arn=solution_version_arn
    )

    # Get recommendations
    user_id = 'da5cc281-7dae-4ef6-9d46-580102ec0784'
    context = {
        'SERVICE_LENGTH': 60,
        'MASSAGE_NAME': 'The NOW 50',
        'CENTER_NAME': 'Roswell',
        'AGE': 30,
        'GENDER': 'Female',
        'ZIPCODE': None,
        'BASE_CENTER': 'Roswell'
    }
    recommendations = personalize.get_recommendations(campaign_arn, user_id, context)
    logger.info(recommendations)


