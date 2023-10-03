from dataiku import api_client
from dataiku.core.message_sender import MessageSender
import time
import dataiku
import pandas as pd

def check_datasets(df, column):
    """
    This function takes in a DataFrame and a column name, and checks the datasets listed in that column.
    If any of the checks fail, the script will end.
    """
    datasets = df[column][df['DATASET_CHECK_Y_N']=='Y'].unique()
    failed_datasets = []
    success_datasets = []
    for dataset_name in datasets:
        try:
            dataset = dataiku.Dataset(dataset_name)
            data = dataset.get_dataframe()
            success_datasets.append(dataset_name)
            
            # Perform checks on the data here
            # If any check fails, raise an exception
        except Exception as e:
            failed_datasets.append(dataset_name)
    return success_datasets, failed_datasets
           # #print(f"Check failed for dataset {dataset_name}: {e}")
           # exit(1)
    #print("All checks passed!")

def run_multiple_scenarios(dataset_config_df, dataset_names_column, scenarios_to_run,project_name,project,Sender,recipients,REPORT_LINK):
    
    success_datasets, failed_datasets = check_datasets(dataset_config_df, dataset_names_column)
    
    recipients = ', '.join(recipients)
    
    if failed_datasets:
        
        failed_datasets_dict = dict([(dset, 'Failed') for dset in failed_datasets])
        
        failed_datasets_urls = ''
        for key, value in failed_datasets_dict.items():
            failed_datasets_urls += f'This dataset {key} has {value} the check: <span style="color:green;"><a href=https://dss-amer-dev.pfizer.com/projects/{project_name}/datasets/{key}/explore/>{key}_URL </a></span><br>\n'
        print(failed_datasets_urls)

        html_message = f"""<!DOCTYPE html>
        <html>
           <body style="font-family:'Calibri'">
              <div style="background-color: #0091ff;color:White;padding:60px;">
              <meta charset="UTF-8">
              <h1>
                 <p>
                    ALERT  
                 </p>
              </h1>
              <div style="background-color:DarkBlue;color:White;padding:60px;">
              <h1>
                 <p>
                 <p style="color:white;">PDA PROJECT: {project_name}
                 </p>
              </h1>
              <div style="background-color:GhostWhite;color:black;padding:40px;">
                 <h2 style="color:black;"> Hi Team,</h2>
                 <h3>
                    <p> 
                       This is to notify that some of the datasets have failed the check. </span><br><br>
                       Below are the details: <br>
                       DSS ENV: AMER DEV BASELINE <br>
                       {failed_datasets_urls} <br>




                    </p>
                 </h3>
                 <h4>
                    Thanks <br>
                    PDA DSS WORKFLOW
                 </h4>
              </div>
           </body>
        </html>"""

        s = MessageSender(channel_id='aessupport@pfizer.com', type='mail-scenario', configuration={})
        s.send(
                sender=Sender, 
                recipient=recipients, 
                subject= f'Email Alert of multiple scenario runs for project {project_name}',
                message=html_message, 
                sendAsHTML=True
                )
    else:
        
        
    
        scenario_runs = []

        for scenario_id in scenarios_to_run:
            scenario = project.get_scenario(scenario_id)

            trigger_fire = scenario.run()
            # Wait for the trigger fire to have actually started a scenario
            scenario_run = trigger_fire.wait_for_scenario_run()
            scenario_runs.append(scenario_run)

        # Poll all scenario runs, until all of them have completed
        while True:
            any_not_complete = False
            for scenario_run in scenario_runs:
                # Update the status from the DSS API
                scenario_run.refresh()
                if scenario_run.running:
                    any_not_complete = True

            if any_not_complete:
                print("At least a scenario is still running...")
            else:
                print("All scenarios are complete")
                break

            # Wait a bit before checking again
            time.sleep(30)

        results_list = dict([(sr.get_info()["trigger"]["scenarioId"], sr.outcome) for sr in scenario_runs])
        scenario_log = dict([(sr.get_info()["trigger"]["scenarioId"], sr.id) for sr in scenario_runs])

        result_output = ''
        for key, value in results_list.items():
            print(f'***********************{value}********************************')
            #if {value}=='SUCCESS':
            result_output += f'The status for scenario {key}: <span style="color:green;">{value}</span>.<br>\n'
            #if {value}=='FAILED':
            #    result_output += f'The status for scenario {key}: <span style="color:red;">{value}</span>.<br>\n'
        print(result_output)

        scenario_log_urls = ''
        for key, value in scenario_log.items():
            scenario_log_urls += f'The log for scenario {key}: <span style="color:green;"><a href=https://dss-amer-dev.pfizer.com/projects/{project_name}/scenarios/{key}/runs/list/{value}>{key}_LOG </a></span><br>\n'
        print(scenario_log_urls)

        html_message = f"""<!DOCTYPE html>
        <html>
           <body style="font-family:'Calibri'">
              <div style="background-color: #0091ff;color:White;padding:60px;">
              <meta charset="UTF-8">
              <h1>
                 <p>
                    ALERT  
                 </p>
              </h1>
              <div style="background-color:DarkBlue;color:White;padding:60px;">
              <h1>
                 <p>
                 <p style="color:white;">PDA PROJECT: {project_name}
                 </p>
              </h1>
              <div style="background-color:GhostWhite;color:black;padding:40px;">
                 <h2 style="color:black;"> Hi Team,</h2>
                 <h3>
                    <p> 
                       This is to notify that execution of following scenarios has completed. </span><br><br>
                       Below are the details: <br>
                       DSS ENV: AMER DEV BASELINE <br>
                       {result_output} <br>
                       {scenario_log_urls}<br>




                    </p>
                 </h3>
                 <h4>
                    Thanks <br>
                    PDA DSS WORKFLOW
                 </h4>
              </div>
           </body>
        </html>"""

        s = MessageSender(channel_id='aessupport@pfizer.com', type='mail-scenario', configuration={})
        
        #RECIPIENTS = {}
        #try:#NOTE JSON LOADS: Array of Objects
       #     RECIPIENTS = recipients
       # except Exception as e:
       #     pass
       # #RECIPIENT_EMAILS = ''
        
        #recipients_emails = ''
       # recipients_emails +=  (',' if recipients != '' else '') + (RECIPIENT_EMAILS)
        #print(recipients)
        s.send(
                sender=Sender, 
                recipient=recipients, 
                subject= f'Email Alert of multiple scenario runs for project {project_name}',
                message=html_message, 
                sendAsHTML=True
                )