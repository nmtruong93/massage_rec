### Step by step
1. Load and preliminary data processing
   ```bash
    python3 recommender/data_loader.py
   ```
2. Build user, item and interaction datasets for aws personalize
   ```bash
    python3 recommender/dataset_builder.py
   ```
3. Create S3 bucket `staging-massage-dataset`, then upload `interaction.csv`, `user.csv` and `item.csv` datasets

4. Run personalization
    ```bash
     python3 recommender/personalize.py
    ```
5. Get recommendations from AWS Personalize
    ```bash
    # Get recommendations in recommender/personalize.py
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
    recommendations = personalize.get_recommendations(campaign_arn, user_id, context)
    logger.info(recommendations)
    ```