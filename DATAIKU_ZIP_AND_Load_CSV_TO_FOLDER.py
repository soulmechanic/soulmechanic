import dataiku
import zipfile
import os

# Get the managed folder
folder = dataiku.Folder("FOLDER_ID")

# Create a new zip file
zip_file = zipfile.ZipFile("my_zip.zip", "w")

# Get the list of files in the managed folder
files = folder.list_paths_in_partition()

# Add each CSV file to the zip file
for file in files:
    if file.endswith(".csv"):
        with folder.get_download_stream(file) as stream:
            zip_file.writestr(file, stream.read())

# Close the zip file
zip_file.close()

# Upload the zip file to the managed folder
with open("my_zip.zip", "rb") as f:
    folder.upload_stream("my_zip.zip", f)

# Delete the local zip file
os.remove("my_zip.zip")



from dataiku import Folder
from datetime import datetime
import zipfile
import os


archive_subfolders = ['Portfolio','Project']


def move_file_dataiku(source_folder, target_folder, archive_subfolders):
    source_folder = Folder(source_folder)
    target_folder = Folder(target_folder)
       
    current_date = datetime.now().strftime('%Y-%m-%d')
    # Locate file in source S3 folder which is older than current date
    files = source_folder.list_paths_in_partition()
    previous_day_files = [file for file in files if datetime.fromtimestamp(source_folder.get_path_details(file)['lastModified']/1000).strftime('%Y-%m-%d') == current_date]
    for subfolder_name in archive_subfolders:
        list_of_csv_files = [file for file in previous_day_files if subfolder_name in file and file.endswith(".csv") ]
        # Move that located file to target S3 folder
        zip_file_name = f'{current_date}_{subfolder_name}.zip'
        # Create a new zip file
        zip_file = zipfile.ZipFile(zip_file_name, "w")

        # Add each CSV file to the zip file
        for file in list_of_csv_files:
            if file.endswith(".csv"):
                with source_folder.get_download_stream(file) as stream:
                    csv_file_name = os.path.basename(file)
                    zip_file.writestr(csv_file_name, stream.read())

        # Close the zip file
        zip_file.close()

        with open(zip_file_name, "rb") as f:
            target_folder.upload_stream('/'+subfolder_name+'/'+zip_file_name, f)
            
        # List the contents of the folder
        contents = target_folder.list_paths_in_partition()
        
        x=0
        # Check if the file exists
        if '/'+subfolder_name+'/'+zip_file_name in contents:
            for to_be_deleted_csv_file in list_of_csv_files:
                source_folder.clear_path(to_be_deleted_csv_file)
            print("File exists")
        else:
            print("File does not exist")
    return 

move_file_dataiku('Source_Folder', 'Target_Folder', archive_subfolders)
