import pandas as pd
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from io import StringIO

def read_csv_from_sharepoint(site_url: str, client_id: str, client_secret: str, relative_url: str) -> pd.DataFrame:
    """
    Reads a CSV file from SharePoint Online using the Office 365 API and converts it to a Pandas DataFrame.

    :param site_url: The URL of the SharePoint site where the file is located.
    :param client_id: The client ID for the Office 365 application.
    :param client_secret: The client secret for the Office 365 application.
    :param relative_url: The relative URL of the file within the SharePoint site.
    :return: A Pandas DataFrame containing the data from the CSV file.
    """
    try:
        ctx_auth = AuthenticationContext(url=site_url)
        if not ctx_auth.acquire_token_for_app(client_id=client_id, client_secret=client_secret):
            print(ctx_auth.get_last_error())
            return

        ctx = ClientContext(site_url, ctx_auth)
        response = File.open_binary(ctx, relative_url)
        csv_data = StringIO(response.content.decode('utf-8'))
        df = pd.read_csv(csv_data)
        return df
    except Exception as e:
        print(f"An error occurred while reading the CSV file from SharePoint: {e}")
