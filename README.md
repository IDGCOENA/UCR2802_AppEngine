**Enable App Engine**

1. Use your GCP Trial to enable App Engine
2. Select your GCP Project that was available "My First Project" or any of the new projects you created.
3. Enable Cloud Shell by selecting "Activate Cloud shell" on your GCP Home Page
4. Once you have the cloud shell provisoned, execute the command **gcloud app create** 
5. If an authorization window appears, click Authorize


**Clone App**

Please execute the following commands from cloud shell 

1. git clone   https://github.com/IDGCOENA/UCR2802_AppEngine.git  -- Clone our App engine Repo
2. Navigate into folder where the repository was cloned. You will find the following files
        a. app.yaml         -->  App Engine Configuration for Deployment
        b. main.py           --> Python App to replicate survey response into HANA Cloud 
        c. requirements.txt  --> List of Package Dependencies         

**Test the App**
1. Create virtual environment qresponse [it could be any environment name]
     virtualenv --python python3 \ ~ /envs/qresponse
2. Activate virtual environment
   **source \ ~ /envs/qresponse/bin/activate**
3. install the requirements 
    **pip install -r requirements.txt**
4. Execute the application
   **python main.py**
5. It should execute succesfully and you shuold see the logs in the cloud shell

**Deploy the App**

1. Execute **gcloud config set project [Your GCP project name]**
2. Deploy by executing  **gcloud app deploy**
3. Navigate to deployed URL and you will see the message "Survey response has been succesfully reploicated in HANA Cloud"
4. You can view the app from App engine dashboard
        
      
