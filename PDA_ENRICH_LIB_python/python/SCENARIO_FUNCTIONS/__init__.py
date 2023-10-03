from dataiku.scenario import Scenario
import dataiku
import pandas as pd

def extract_elements(json_obj, keys):
    result = {}
    
    def extract(json_obj, keys, result):
        if isinstance(json_obj, dict):
            for key, value in json_obj.items():
                if key in keys:
                    result[key] = value
                else:
                    extract(value, keys, result)
        elif isinstance(json_obj, list):
            for item in json_obj:
                extract(item, keys, result)
    
    extract(json_obj, keys, result)
    return result


def run_scenario_from_point_failure(scenario_name, scenario_first_step, project_details='Default'):
    '''
    Runs a scenario if failed or aborted from step which it failed.
    
    param scenario_name: A valid scenario name
    param scenario_first_step:  A valid first step name of the scenario.
    param project_details: if project_details is default it will get the default project name and if mentioned it will run in that particular project.
    
    '''
        
    try:
        # get client and Scenario details
        client = dataiku.api_client()
        if project_details=='Default':
            project = client.get_default_project()
        else:
            project = client.get_project(project_details)

        scenario = project.get_scenario(scenario_name)
        project_variables = project.get_variables()
        last_run = scenario.get_last_finished_run()
        
        # check if the last finished scenario run outcome is 'FAILED' or 'ABORTED' only then perform next steps
        if last_run.outcome in ['FAILED','ABORTED']:
            last_run_details = last_run.get_details()
            scenario_last_step = last_run_details.last_step
            failed_step = extract_elements(scenario_last_step, 'step')
            last_failed_step_name = failed_step['step']['name']
            scenario_end_step = scenario_name+"_SCENARIO_START_FROM_STEP"
            project_variables["local"][scenario_end_step] = {last_failed_step_name: 1}
            project.set_variables(project_variables)
            
        # else if the last finished scenario run outcome is not 'FAILED' or 'ABORTED' only then perform next steps
        else:
            scenario_end_step = scenario_name+"_SCENARIO_START_FROM_STEP"
            project_variables["local"][scenario_end_step] = {scenario_first_step: 1}
            project.set_variables(project_variables)
            
    except Exception as e:
        print(f"An error occurred while funtion run_scenario_from_point_failure: {e}")
            
            
    