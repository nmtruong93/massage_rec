### Step by step
1. Create a virtual environment
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
2. Change `.env.example` to `.env.staging` or `.env.prod` and fill in environment variables

3. Create S3 bucket `staging-massage-dataset` in staging env or `prod-massage-dataset` in production env

4. Run full pipeline for training and deploying model
    ```bash
    python recommender/pipeline_train.py
    ```
5. Pipeline for get recommendations
    ```bash
    python recommender/pipeline_inference.py
    ```
6. Get recommendations from AWS Personalize
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