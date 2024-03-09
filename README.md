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