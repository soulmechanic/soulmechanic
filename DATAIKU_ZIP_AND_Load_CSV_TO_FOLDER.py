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
