# Flask Web Server for Predicting Top N Videos based on a Text Query and Adding Support Data to Improve Search

## Features
Transformers for Vectorizing and Similarity Matching and returns Top N results

Based on selected output, added this inforamtion for better classification going forward(Feedback)


# Steps to Run Server Locally
1.) Get the code from - https://github.com/vj-m/AF_Pro_NLP_APP

2.) cd into '/nlp_app' directory
```commandline
cd nlp_app
```

3.) Create a virtual Environment and Install packages needed using requirements.txt file

For MacOS to create and enter Virtual Environment
```commandline
python3 -m venv env
source env/bin/activate
```

```
pip install -r requirements.txt
``` 

4.) For Mac/Linux to run the server locally



Changes needed in api.py to run locally as shown below
```commandline
app.run(host="0.0.0.0", port=80)
#serve(app, host = '0.0.0.0', port = 80)
```

To Run
```commandline
export FLASK_APP=api.py
```
```commandline
flask run
```