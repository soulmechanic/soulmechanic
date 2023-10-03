'''
custom Python step at the end of your scenario, with the “Run this step” option set to “If a prior step failed”. You can then use the Dataiku API to unset the auto-trigger field of your scenario. Here’s an example code snippet that you can use, filling in YOUR_PROJECT_KEY and YOUR_SCENARIO_ID with the proper values 1:
'''

from dataiku import api_client
project = api_client().get_project(YOUR_PROJECT_KEY)
scenario_settings = project.get_scenario(YOUR_SCENARIO_ID).get_settings()
scenario_settings.active = False
scenario_settings.save()

'''
You can use the Dataiku API to rerun a scenario until it finishes successfully. Here’s an example code snippet that you can use, filling in YOUR_PROJECT_KEY, YOUR_SCENARIO_ID, and MAX_TRIES with the proper values:
'''
from dataiku import api_client
import time

project = api_client().get_project(YOUR_PROJECT_KEY)
scenario = project.get_scenario(YOUR_SCENARIO_ID)

MAX_TRIES = 5
tries = 0
while tries < MAX_TRIES:
    run = scenario.run_and_wait()
    if run.get_info()["result"]["outcome"] == "SUCCESS":
        print("Scenario finished successfully")
        break
    else:
        print("Scenario failed, retrying...")
        tries += 1
        time.sleep(60) # wait for 60 seconds before retrying
else:
    print("Scenario failed after {} tries".format(MAX_TRIES))

	
'''
Sure! Here’s an example of custom code that you can use to send notifications about scenario progress in Dataiku. This code uses the dataikuapi.dss.mail.DSSMail class to send an email notification when a scenario fails:
'''


from dataiku import api_client
from dataikuapi.dss.mail import DSSMail

YOUR_PROJECT_KEY = "YOUR_PROJECT_KEY"
YOUR_SCENARIO_ID = "YOUR_SCENARIO_ID"
YOUR_EMAIL = "YOUR_EMAIL"

client = api_client()
project = client.get_project(YOUR_PROJECT_KEY)
scenario = project.get_scenario(YOUR_SCENARIO_ID)

run = scenario.run_and_wait()
if run.get_info()["result"]["outcome"] != "SUCCESS":
    mail = DSSMail(subject="Scenario failed", body="The scenario {} in project {} has failed".format(YOUR_SCENARIO_ID, YOUR_PROJECT_KEY))
    mail.add_to(YOUR_EMAIL)
    mail.send()
