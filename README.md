### Training process

1. Create folder named `data` in the root directory
   `data` folder directory should be like this
   ```
    data
    ├──trainingData.csv
    ├──trainingLabels.csv
   
2. Install python 3.10
3. Create a virtual environment
    ```bash
    python -m venv venv
    ```
    ```bash
    source venv/bin/activate
    ```
    ```bash
    python -m pip install --upgrade pip
    ```
4. Install the required packages
    ```bash
    pip install -r requirements.txt
    ```
5. Run the [pipeline_train.py](gender_prediction%2Fpipeline_train.py)
   - Using IDE
     - Open the project in your favorite IDE
     - Run the file [pipeline_train.py](gender_prediction%2Fpipeline_train.py)
   - Using command line
     ```bash
      cd gender_classification
        
      export PYTHONPATH="${PYTHONPATH}:${PWD}" 
        
      python gender_prediction/pipeline_train.py
     ```
   Please note that the training process will take a few minutes to complete. 
   We use Optuna to find the best hyperparameters for the model.
    

6. The trained model will be saved in the `tmp` folder

### Prediction process
Run the file [pipeline_prediction.py](gender_prediction%2Fpipeline_prediction.py)

Label:
- Female: 0.0
- Male: 1.0

Overall Gender Score: 0.8257492829234192

Overall metrics:
```angular2html
              precision    recall  f1-score   support

         0.0       0.91      0.98      0.94     11703
         1.0       0.89      0.68      0.77      3297

    accuracy                           0.91     15000
   macro avg       0.90      0.83      0.86     15000
weighted avg       0.91      0.91      0.91     15000
```